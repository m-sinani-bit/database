import sqlite3
from datetime import datetime,timedelta
import numpy as np
import matplotlib.pyplot as plt
from statistics import mean, median
import csv
import numbers
import os
from matplotlib.ticker import PercentFormatter
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import mysql.connector
from mysql.connector import Error


from reportlab.lib.colors import Color, gray, lightgrey, black, white,linen, lightblue, gainsboro
PATHLightBlue = Color((178/255), (231/255), (250.0/255), 1)

server_config = {
    'user': 'DataEvalAdmin',
    'password': '#########',
    'host': '127.0.0.1',
    'ssl_disabled': True
}

database_name = "PATHParticipantDB"

def active_clients(start_date, end_date, program_id=None, department=None, region=None, program_type=None, server=server_config,db=database_name):
    conn=mysql.connector.connect(**server)
    cursor=conn.cursor()

    cursor.execute(f"USE `{db}`")


    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
    '''
    
    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['%s' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['%s' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['%s' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['%s' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    cursor.execute(sql, filter_params)

    result = cursor.fetchone()[0]
    conn.close()
    return result


def personal_data_quality(start_date, end_date, program_id=None, department= None, region=None, program_type=None,server=server_config,db=database_name):
    conn = mysql.connector.connect(**server)
    cursor = conn.cursor()

    cursor.execute(f"USE `{db}`")
    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID) 
            FROM Enrollment
            LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
            WHERE Enrollment.EntryDate <= %s
            AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
            AND (Client.NameDataQuality > 1 OR 
                        (Client.SSNDataQuality > 2 OR (LENGTH(Client.SSN) <> 9) OR (LENGTH(Client.SSN) = 9 AND (
                                            Client.SSN LIKE '000000000'
                                            OR Client.SSN LIKE '111111111'
                                            OR Client.SSN LIKE '222222222'
                                            OR Client.SSN LIKE '333333333'
                                            OR Client.SSN LIKE '444444444'
                                            OR Client.SSN LIKE '555555555'
                                            OR Client.SSN LIKE '666666666'
                                            OR Client.SSN LIKE '777777777'
                                            OR Client.SSN LIKE '888888888'
                                            OR Client.SSN LIKE '999999999'
                                            OR Client.SSN LIKE '123456789'
                                            OR Client.SSN LIKE '234567890'
                                            OR Client.SSN LIKE '987654321')
                                        OR (SUBSTR(Client.SSN, 1, 3) IN ('000', '666', '900')
                                            OR SUBSTR(Client.SSN, 4, 6) = '00')
                            ) OR
                            (
                                (Client.DOBDataQuality IN (8, 9, 99))
                                OR Client.DOBDataQuality = 2
                                OR (
                                    Client.DOB < '1915-01-01' -- Prior to 1/1/1915
                                    OR Client.DOB > Enrollment.EntryDate -- After the record creation date
                                )
                            )
                            ) OR
                        (Client.RaceNone >= 8) OR
                        (Client.GenderNone >= 8)
            )
        '''

    sql_active_non_outreach = """
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN `Exit` on Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
    """

    sql_active_outreach = """
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN `Exit` on Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND Enrollment.DateOfEngagement <= %s
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'
    """

    filter_params = [end_date, start_date]
    outreach_params = [end_date, start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['%s' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)
        outreach_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['%s' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)
        outreach_params.extend(department)

    if region is not None:
        placeholders = ','.join(['%s' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)
        outreach_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['%s' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        sql_active_non_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        sql_active_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)
        outreach_params.extend(program_type)
        
    cursor.execute(sql, filter_params)
    total_score = cursor.fetchone()[0]
    
    cursor.execute(sql_active_outreach, outreach_params)
    total_outreach = cursor.fetchone()[0]
    
    cursor.execute(sql_active_non_outreach, filter_params)
    total_non_outreach = cursor.fetchone()[0]
    
    conn.close()

    total_enrollments = total_outreach + total_non_outreach

    if isinstance(total_score, numbers.Number):                       
        if total_enrollments > 0:
            if 1-(total_score/total_enrollments) <= 0:
                return 0
            else:
                return 1-(total_score/total_enrollments)
        else:
            return 0
    else:
        return None
        
def universal_data_quality(start_date, end_date, program_id=None, department=None, region=None, program_type=None, server=server_config, db=database_name):
    conn = mysql.connector.connect(**server)
    cursor = conn.cursor()

    cursor.execute(f"USE `{db}`")
    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID) 
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND (
            ((Client.VeteranStatus = 8 OR Client.VeteranStatus = 9) 
                AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18)
            OR ((Client.VeteranStatus = 99 OR Client.VeteranStatus IS NULL) 
                AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18)
            OR (Client.VeteranStatus = 1 
                AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) < 18)
            OR (Enrollment.EntryDate > Exit.ExitDate)
            OR (Enrollment.RelationshipToHoH = 99 OR Enrollment.RelationshipToHoH IS NULL)
            OR (Enrollment.RelationshipToHoH NOT BETWEEN 1 AND 5)
            OR (Enrollment.RelationshipToHoH = 1 
                AND Enrollment.EnrollmentCoC IS NULL)
            OR (Enrollment.RelationshipToHoH = 1 
                AND Enrollment.EnrollmentCoC NOT IN (
                    'CA-600', 'CA-601', 'CA-606', 'CA-602', 'CA-500', 'CA-612', 'CA-614', 'CA-607'
                ))
            OR (Enrollment.DisablingCondition = 8 OR Enrollment.DisablingCondition = 9)
            OR (Enrollment.DisablingCondition = 99 OR Enrollment.DisablingCondition IS NULL)
        )
        '''

    sql_active_non_outreach = """
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN `Exit` on Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
    """

    sql_active_outreach = """
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN `Exit` on Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND Enrollment.DateOfEngagement <= %s
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'
    """

    filter_params = [end_date, start_date]
    outreach_params = [end_date, start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['%s' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)
        outreach_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['%s' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)
        outreach_params.extend(department)

    if region is not None:
        placeholders = ','.join(['%s' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)
        outreach_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['%s' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        sql_active_non_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        sql_active_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)
        outreach_params.extend(program_type)
        
    cursor.execute(sql, filter_params)
    total_score = cursor.fetchone()[0]
    
    cursor.execute(sql_active_outreach, outreach_params)
    total_outreach = cursor.fetchone()[0]
    
    cursor.execute(sql_active_non_outreach, filter_params)
    total_non_outreach = cursor.fetchone()[0]
    
    conn.close()

    total_enrollments = total_outreach + total_non_outreach

    if isinstance(total_score, numbers.Number):                       
        if total_enrollments > 0:
            if 1-(total_score/total_enrollments) <= 0:
                return 0
            else:
                return 1-(total_score/total_enrollments)
        else:
            return 0
    else:
        return None

class Name():

    def name_total_accuracy(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")

        
        sql_numerator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND ((Client.NameDataQuality in (2,8,9,99))
                OR Client.FirstName IS NULL
                OR Client.LastName IS NULL)"""
                
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.UniqueIdentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on UniqueID.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND ((Client.NameDataQuality in (2,8,9,99))
                OR Client.FirstName IS NULL
                OR Client.LastName IS NULL)'''

        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.UniqueIdentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on UniqueID.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND ((Client.NameDataQuality in (2,8,9,99))
                OR Client.FirstName IS NULL
                OR Client.LastName IS NULL)'''
                                

        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list
            
    
    def name_client_refused_doesnt_know(self,start_date, end_date,output=None,program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND (Client.NameDataQuality in (8,9))'''
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.UniqueIdentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND (Client.NameDataQuality in (8,9))'''
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND (Client.NameDataQuality in (8,9))'''
                
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list
        
    def name_missing(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <=%s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND ((Client.NameDataQuality = 99)
                OR Client.FirstName IS NULL
                OR Client.LastName IS NULL)'''
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND ((Client.NameDataQuality = 99)
                OR Client.FirstName IS NULL
                OR Client.LastName IS NULL)'''

        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND ((Client.NameDataQuality = 99)
                OR Client.FirstName IS NULL
                OR Client.LastName IS NULL)'''                
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list

    def name_data_accuracy(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND (Client.NameDataQuality = 2)'''
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND (Client.NameDataQuality = 2)'''          
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND (Client.NameDataQuality = 2)'''     
                

        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list
    
class SSN():

    def ssn_total_accuracy(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND (
            Client.SSNDataQuality IN (8, 9, 99)
            OR Client.SSN IS NULL
            OR (
                (Client.SSNDataQuality = 2)
                OR 
                (
                    Client.SSNDataQuality = 1 
                    AND LENGTH(Client.SSN) = 9
                    AND (
                        Client.SSN LIKE '000000000'
                        OR Client.SSN LIKE '111111111'
                        OR Client.SSN LIKE '222222222'
                        OR Client.SSN LIKE '333333333'
                        OR Client.SSN LIKE '444444444'
                        OR Client.SSN LIKE '555555555'
                        OR Client.SSN LIKE '666666666'
                        OR Client.SSN LIKE '777777777'
                        OR Client.SSN LIKE '888888888'
                        OR Client.SSN LIKE '999999999'
                        OR Client.SSN LIKE '123456789'
                        OR Client.SSN LIKE '234567890'
                        OR Client.SSN LIKE '987654321'
                        OR Client.SSN LIKE '00000____'
                    )
                )
            ))
        '''
        
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND (
            Client.SSNDataQuality IN (8, 9, 99)
            OR Client.SSN IS NULL
            OR (
                (Client.SSNDataQuality = 2)
                OR 
                (
                    Client.SSNDataQuality = 1 
                    AND LENGTH(Client.SSN) = 9
                    AND (
                        Client.SSN LIKE '000000000'
                        OR Client.SSN LIKE '111111111'
                        OR Client.SSN LIKE '222222222'
                        OR Client.SSN LIKE '333333333'
                        OR Client.SSN LIKE '444444444'
                        OR Client.SSN LIKE '555555555'
                        OR Client.SSN LIKE '666666666'
                        OR Client.SSN LIKE '777777777'
                        OR Client.SSN LIKE '888888888'
                        OR Client.SSN LIKE '999999999'
                        OR Client.SSN LIKE '123456789'
                        OR Client.SSN LIKE '234567890'
                        OR Client.SSN LIKE '987654321'
                    )
                )
            ))
        '''

        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND (
            Client.SSNDataQuality IN (8, 9, 99)
            OR Client.SSN IS NULL
            OR (
                (Client.SSNDataQuality = 2)
                OR 
                (
                    Client.SSNDataQuality = 1 
                    AND LENGTH(Client.SSN) = 9
                    AND (
                        Client.SSN LIKE '000000000'
                        OR Client.SSN LIKE '111111111'
                        OR Client.SSN LIKE '222222222'
                        OR Client.SSN LIKE '333333333'
                        OR Client.SSN LIKE '444444444'
                        OR Client.SSN LIKE '555555555'
                        OR Client.SSN LIKE '666666666'
                        OR Client.SSN LIKE '777777777'
                        OR Client.SSN LIKE '888888888'
                        OR Client.SSN LIKE '999999999'
                        OR Client.SSN LIKE '123456789'
                        OR Client.SSN LIKE '234567890'
                        OR Client.SSN LIKE '987654321'
                    )
                )
            ))
        '''                
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list
            
    def ssn_client_refused_doesnt_know(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None,server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        sql_numerator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN  `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND (Client.SSNDataQuality in (8,9))
        """

        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND (Client.SSNDataQuality in (8,9))'''
        
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND (Client.SSNDataQuality in (8,9))'''
                
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list

    def ssn_missing(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
            conn=mysql.connector.connect(**server)
            cursor=conn.cursor()

            cursor.execute(f"USE `{db}`")
            
            sql_numerator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                    AND  (Client.SSNDataQuality =99
                    OR Client.SSN IS NULL)"""       
                    
            sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                    AND  (Client.SSNDataQuality =99
                    OR Client.SSN IS NULL)'''   
                    
            sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                    AND  (Client.SSNDataQuality =99
                    OR Client.SSN IS NULL)'''  
            sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """

            filter_params = [end_date, start_date]
            outreach_params = [end_date, end_date, start_date]

            if program_id is not None:
                placeholders = ','.join(['%s' for _ in program_id])
                sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                filter_params.extend(program_id)
                outreach_params.extend(program_id)

            if department is not None:
                placeholders = ','.join(['%s' for _ in department])
                sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                filter_params.extend(department)
                outreach_params.extend(department)

            if region is not None:
                placeholders = ','.join(['%s' for _ in region])
                sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                filter_params.extend(region)
                outreach_params.extend(region)

            if program_type is not None:
                placeholders = ','.join(['%s' for _ in program_type])
                sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                filter_params.extend(program_type)
                outreach_params.extend(program_type)
                
            cursor.execute(sql_numerator_combined, outreach_params)
            scores=cursor.fetchone()
            non_outreach_score=scores[0]
            outreach_scores=scores[1]
            
            cursor.execute(sql_denominator_combined, outreach_params)
            result=cursor.fetchone()
            total_non_outreach = result[0]
            total_outreach=result[1]
            
            cursor.execute(sql_non_outreach_list, filter_params)
            non_outreach_pt_list = cursor.fetchall()
            
            cursor.execute(sql_outreach_list, outreach_params)
            outreach_pt_list = cursor.fetchall()
            
            conn.close()

            total_score=non_outreach_score+outreach_scores
            total_enrollments = total_outreach + total_non_outreach
            full_list=non_outreach_pt_list+outreach_pt_list
            if output==None:
                if isinstance(total_score, numbers.Number):                       
                    if total_enrollments > 0:
                        if 1-(total_score/total_enrollments) <= 0:
                            return 0
                        else:
                            return 1-(total_score/total_enrollments)
                    else:
                        return 0
                else:
                    return None
            elif output =="list":
                return full_list
            
    def ssn_data_accuracy(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None,server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        sql_numerator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
            AND (
                (Client.SSNDataQuality = 2)
                OR 
                (
                    Client.SSNDataQuality = 1 
                    AND LENGTH(Client.SSN) = 9
                    AND (
                        Client.SSN LIKE '000000000'
                        OR Client.SSN LIKE '111111111'
                        OR Client.SSN LIKE '222222222'
                        OR Client.SSN LIKE '333333333'
                        OR Client.SSN LIKE '444444444'
                        OR Client.SSN LIKE '555555555'
                        OR Client.SSN LIKE '666666666'
                        OR Client.SSN LIKE '777777777'
                        OR Client.SSN LIKE '888888888'
                        OR Client.SSN LIKE '999999999'
                        OR Client.SSN LIKE '123456789'
                        OR Client.SSN LIKE '234567890'
                        OR Client.SSN LIKE '987654321'
                        OR Client.SSN LIKE '00000____'
                    )
                )
            )
            """
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
            AND (
                (Client.SSNDataQuality = 2)
                OR 
                (
                    Client.SSNDataQuality = 1 
                    AND LENGTH(Client.SSN) = 9
                    AND (
                        Client.SSN LIKE '000000000'
                        OR Client.SSN LIKE '111111111'
                        OR Client.SSN LIKE '222222222'
                        OR Client.SSN LIKE '333333333'
                        OR Client.SSN LIKE '444444444'
                        OR Client.SSN LIKE '555555555'
                        OR Client.SSN LIKE '666666666'
                        OR Client.SSN LIKE '777777777'
                        OR Client.SSN LIKE '888888888'
                        OR Client.SSN LIKE '999999999'
                        OR Client.SSN LIKE '123456789'
                        OR Client.SSN LIKE '234567890'
                        OR Client.SSN LIKE '987654321'
                        OR Client.SSN LIKE '00000____'
                    )
                )
            )
            '''

        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
            AND (
                (Client.SSNDataQuality = 2)
                OR 
                (
                    Client.SSNDataQuality = 1 
                    AND LENGTH(Client.SSN) = 9
                    AND (
                        Client.SSN LIKE '000000000'
                        OR Client.SSN LIKE '111111111'
                        OR Client.SSN LIKE '222222222'
                        OR Client.SSN LIKE '333333333'
                        OR Client.SSN LIKE '444444444'
                        OR Client.SSN LIKE '555555555'
                        OR Client.SSN LIKE '666666666'
                        OR Client.SSN LIKE '777777777'
                        OR Client.SSN LIKE '888888888'
                        OR Client.SSN LIKE '999999999'
                        OR Client.SSN LIKE '123456789'
                        OR Client.SSN LIKE '234567890'
                        OR Client.SSN LIKE '987654321'
                        OR Client.SSN LIKE '00000____'
                    )
                )
            )
            '''                
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list 
class DOB():
            
    def dob_total_accuracy(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND  (Client.DOBDataQuality IN (8, 9, 99)
                OR Client.DOBDataQuality = 2
                OR (Client.DOB < '1915-01-01') -- Prior to 1/1/1915
                OR (Client.DOB > Enrollment.EntryDate) -- After the record creation date
                )'''
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND  (Client.DOBDataQuality IN (8, 9, 99)
                OR Client.DOB IS NULL
                OR Client.DOBDataQuality = 2
                OR (Client.DOB < '1915-01-01') -- Prior to 1/1/1915
                OR (Client.DOB > Enrollment.EntryDate) -- After the record creation date
                )'''
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND  (Client.DOBDataQuality IN (8, 9, 99)
                OR Client.DOB IS NULL
                OR Client.DOBDataQuality = 2
                OR (Client.DOB < '1915-01-01') -- Prior to 1/1/1915
                OR (Client.DOB > Enrollment.EntryDate) -- After the record creation date
                )'''                        

        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list

    def dob_client_refused_doesnt_know(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None,server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND  (Client.DOBDataQuality IN (8, 9))'''
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND  (Client.DOBDataQuality IN (8, 9))'''
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND  (Client.DOBDataQuality IN (8, 9))'''                                     
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list

    def dob_missing(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None,server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND  (Client.DOBDataQuality =99)'''
                
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND  (Client.DOBDataQuality =99)'''
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND  (Client.DOBDataQuality =99)'''
                
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list
        
    def dob_data_accuracy(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND (Client.DOBDataQuality = 2
                OR (Client.DOB < '1915-01-01') -- Prior to 1/1/1915
                OR (Client.DOB > Enrollment.EntryDate))'''
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND (Client.DOBDataQuality = 2
                OR (Client.DOB < '1915-01-01') -- Prior to 1/1/1915
                OR (Client.DOB > Enrollment.EntryDate))'''
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND (Client.DOBDataQuality = 2
                OR (Client.DOB < '1915-01-01') -- Prior to 1/1/1915
                OR (Client.DOB > Enrollment.EntryDate))'''               
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list
class Race():
    def race_total_accuracy(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND  Client.RaceNone in (8,9,99)'''
        
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND  Client.RaceNone in (8,9,99)'''
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND  Client.RaceNone in (8,9,99)'''                
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list

    def race_client_refused_doesnt_know(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND Client.RaceNone in (8,9)'''
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND Client.RaceNone in (8,9)'''
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND Client.RaceNone in (8,9)'''                
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list
        
    def race_missing(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
            conn=mysql.connector.connect(**server)
            cursor=conn.cursor()

            cursor.execute(f"USE `{db}`")
                
            sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                    AND  Client.RaceNone =99'''
                    
            sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                    AND  Client.RaceNone =99'''
            sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                    AND  Client.RaceNone =99'''                    
            sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """

            filter_params = [end_date, start_date]
            outreach_params = [end_date, end_date, start_date]

            if program_id is not None:
                placeholders = ','.join(['%s' for _ in program_id])
                sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                filter_params.extend(program_id)
                outreach_params.extend(program_id)

            if department is not None:
                placeholders = ','.join(['%s' for _ in department])
                sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                filter_params.extend(department)
                outreach_params.extend(department)

            if region is not None:
                placeholders = ','.join(['%s' for _ in region])
                sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                filter_params.extend(region)
                outreach_params.extend(region)

            if program_type is not None:
                placeholders = ','.join(['%s' for _ in program_type])
                sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                filter_params.extend(program_type)
                outreach_params.extend(program_type)
                
            cursor.execute(sql_numerator_combined, outreach_params)
            scores=cursor.fetchone()
            non_outreach_score=scores[0]
            outreach_scores=scores[1]
            
            cursor.execute(sql_denominator_combined, outreach_params)
            result=cursor.fetchone()
            total_non_outreach = result[0]
            total_outreach=result[1]
            
            cursor.execute(sql_non_outreach_list, filter_params)
            non_outreach_pt_list = cursor.fetchall()
            
            cursor.execute(sql_outreach_list, outreach_params)
            outreach_pt_list = cursor.fetchall()
            
            conn.close()

            total_score=non_outreach_score+outreach_scores
            total_enrollments = total_outreach + total_non_outreach
            full_list=non_outreach_pt_list+outreach_pt_list
            if output==None:
                if isinstance(total_score, numbers.Number):                       
                    if total_enrollments > 0:
                        if 1-(total_score/total_enrollments) <= 0:
                            return 0
                        else:
                            return 1-(total_score/total_enrollments)
                    else:
                        return 0
                else:
                    return None
            elif output =="list":
                return full_list
class Gender():
    def gender_total_accuracy(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND  (Client.GenderNone >= 8)'''
                
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND  (Client.GenderNone >= 8)'''
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND  (Client.GenderNone >= 8)'''                
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """


        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list

    def gender_client_refused_doesnt_know(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND  (Client.GenderNone in(8,9))'''
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND  (Client.GenderNone in(8,9))'''
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND  (Client.GenderNone in(8,9))'''                           
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list

    def gender_missing(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None,server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND  (Client.GenderNone =99)'''
                
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND  (Client.GenderNone =99)'''
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND  (Client.GenderNone =99)'''                
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list

class Veteran():
    def veteran_total_accuracy(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None,server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
                                AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND (
            (Client.VeteranStatus IN (8, 9) AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18) 
            OR ((Client.VeteranStatus = 99 OR Client.VeteranStatus IS NULL) 
                AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18) 
            OR (Client.VeteranStatus = 1 
                AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) < 18)
        )
        '''

                
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN  `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND (
            (Client.VeteranStatus IN (8, 9) AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18) 
            OR ((Client.VeteranStatus = 99 OR Client.VeteranStatus IS NULL) 
                AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18) 
            OR (Client.VeteranStatus = 1 
                AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) < 18)
        )'''
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND (
            (Client.VeteranStatus IN (8, 9) AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18) 
            OR ((Client.VeteranStatus = 99 OR Client.VeteranStatus IS NULL) 
                AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18) 
            OR (Client.VeteranStatus = 1 
                AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) < 18)
        )'''                
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                        AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18

        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list
        
    def veteran_client_refused_doesnt_know(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18
                AND (Client.VeteranStatus in (8,9) ) '''
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18
                AND (Client.VeteranStatus in (8,9) ) '''
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18
                AND (Client.VeteranStatus in (8,9) ) '''              
    
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                        AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list

    def veteran_missing(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
            conn=mysql.connector.connect(**server)
            cursor=conn.cursor()

            cursor.execute(f"USE `{db}`")
            
            sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                    AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18
                    AND (Client.VeteranStatus = 99 
                    OR Client.VeteranStatus IS NULL)'''
                    
            sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                    AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18
                    AND (Client.VeteranStatus = 99 
                    OR Client.VeteranStatus IS NULL)'''
            sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                    AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18
                    AND (Client.VeteranStatus = 99 
                    OR Client.VeteranStatus IS NULL)'''
                    
            sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                        AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18
        """

            filter_params = [end_date, start_date]
            outreach_params = [end_date, end_date, start_date]

            if program_id is not None:
                placeholders = ','.join(['%s' for _ in program_id])
                sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                filter_params.extend(program_id)
                outreach_params.extend(program_id)

            if department is not None:
                placeholders = ','.join(['%s' for _ in department])
                sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                filter_params.extend(department)
                outreach_params.extend(department)

            if region is not None:
                placeholders = ','.join(['%s' for _ in region])
                sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                filter_params.extend(region)
                outreach_params.extend(region)

            if program_type is not None:
                placeholders = ','.join(['%s' for _ in program_type])
                sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                filter_params.extend(program_type)
                outreach_params.extend(program_type)
                
            cursor.execute(sql_numerator_combined, outreach_params)
            scores=cursor.fetchone()
            non_outreach_score=scores[0]
            outreach_scores=scores[1]
            
            cursor.execute(sql_denominator_combined, outreach_params)
            result=cursor.fetchone()
            total_non_outreach = result[0]
            total_outreach=result[1]
            
            cursor.execute(sql_non_outreach_list, filter_params)
            non_outreach_pt_list = cursor.fetchall()
            
            cursor.execute(sql_outreach_list, outreach_params)
            outreach_pt_list = cursor.fetchall()
            
            conn.close()

            total_score=non_outreach_score+outreach_scores
            total_enrollments = total_outreach + total_non_outreach
            full_list=non_outreach_pt_list+outreach_pt_list
            if output==None:
                if isinstance(total_score, numbers.Number):                       
                    if total_enrollments > 0:
                        if 1-(total_score/total_enrollments) <= 0:
                            return 0
                        else:
                            return 1-(total_score/total_enrollments)
                    else:
                        return 0
                else:
                    return None
            elif output =="list":
                return full_list


    #HMIS does not allow for this to be a possibility, so it has been removed from reporting
    def veteran_data_accuracy(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
            conn=mysql.connector.connect(**server)
            cursor=conn.cursor()

            cursor.execute(f"USE `{db}`")
            
            sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                    AND (Client.VeteranStatus = 1 AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) < 18 )'''
            sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                    AND (Client.VeteranStatus = 1 AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) < 18 )'''
            sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                    AND (Client.VeteranStatus = 1 AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) < 18 )'''                 
            sql_denominator_combined = """
            SELECT 
                COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
                COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
            FROM Enrollment
            LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                        AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18
            """

            filter_params = [end_date, start_date]
            outreach_params = [end_date, end_date, start_date]

            if program_id is not None:
                placeholders = ','.join(['%s' for _ in program_id])
                sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                filter_params.extend(program_id)
                outreach_params.extend(program_id)

            if department is not None:
                placeholders = ','.join(['%s' for _ in department])
                sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                filter_params.extend(department)
                outreach_params.extend(department)

            if region is not None:
                placeholders = ','.join(['%s' for _ in region])
                sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                filter_params.extend(region)
                outreach_params.extend(region)

            if program_type is not None:
                placeholders = ','.join(['%s' for _ in program_type])
                sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                filter_params.extend(program_type)
                outreach_params.extend(program_type)
                
            cursor.execute(sql_numerator_combined, outreach_params)
            scores=cursor.fetchone()
            non_outreach_score=scores[0]
            outreach_scores=scores[1]
            
            cursor.execute(sql_denominator_combined, outreach_params)
            result=cursor.fetchone()
            total_non_outreach = result[0]
            total_outreach=result[1]
            
            cursor.execute(sql_non_outreach_list, filter_params)
            non_outreach_pt_list = cursor.fetchall()
            
            cursor.execute(sql_outreach_list, outreach_params)
            outreach_pt_list = cursor.fetchall()
            
            conn.close()

            total_score=non_outreach_score+outreach_scores
            total_enrollments = total_outreach + total_non_outreach
            full_list=non_outreach_pt_list+outreach_pt_list
            if output==None:
                if isinstance(total_score, numbers.Number):                       
                    if total_enrollments > 0:
                        if 1-(total_score/total_enrollments) <= 0:
                            return 0
                        else:
                            return 1-(total_score/total_enrollments)
                    else:
                        return 0
                else:
                    return None
            elif output =="list":
                return full_list

class Disabling():
    
    def disabling_condition_total_accuracy(self,start_date, end_date, output=None,program_id=None, department=None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")

        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN Disabilities ON Enrollment.PersonalID = Disabilities.PersonalID
        WHERE Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND (Enrollment.DisablingCondition in (8,9,99)
        OR (Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType = 6 AND Disabilities.DisabilityResponse=1)
        OR (Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType=8 AND Disabilities.DisabilityResponse=1)
        OR (Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType IN (5,7,9,10) AND Disabilities.DisabilityResponse=1 AND Disabilities.IndefiniteAndImpairs=1))
        '''
        
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN Disabilities ON Enrollment.PersonalID = Disabilities.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND (Enrollment.DisablingCondition in (8,9,99)
        OR (Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType = 6 AND Disabilities.DisabilityResponse=1)
        OR (Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType=8 AND Disabilities.DisabilityResponse=1)
        OR (Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType IN (5,7,9,10) AND Disabilities.DisabilityResponse=1 AND Disabilities.IndefiniteAndImpairs=1))
        '''

        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN Disabilities ON Enrollment.PersonalID = Disabilities.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND (Enrollment.DisablingCondition in (8,9,99)
        OR (Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType = 6 AND Disabilities.DisabilityResponse=1)
        OR (Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType=8 AND Disabilities.DisabilityResponse=1)
        OR (Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType IN (5,7,9,10) AND Disabilities.DisabilityResponse=1 AND Disabilities.IndefiniteAndImpairs=1))
        '''
        
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list

    def disabling_condition_client_refused(self,start_date, end_date, output=None,program_id=None, department=None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")

        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN Disabilities ON Enrollment.PersonalID = Disabilities.PersonalID
        WHERE Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND  Enrollment.DisablingCondition in (8,9)
        '''
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN Disabilities ON Enrollment.PersonalID = Disabilities.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND  Enrollment.DisablingCondition in (8,9)
        '''

        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN Disabilities ON Enrollment.PersonalID = Disabilities.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND  Enrollment.DisablingCondition in (8,9)
        '''        
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list

    def disabling_condition_missing(self,start_date, end_date, output=None,program_id=None, department=None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")

        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN Disabilities ON Enrollment.PersonalID = Disabilities.PersonalID
        WHERE Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND  Enrollment.DisablingCondition=99
        '''
        
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN Disabilities ON Enrollment.PersonalID = Disabilities.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND  Enrollment.DisablingCondition=99
        '''

        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN Disabilities ON Enrollment.PersonalID = Disabilities.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND  Enrollment.DisablingCondition=99
        '''  
        
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """
        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list
        
    def disabling_condition_data_accuracy(self,start_date, end_date,output=None, program_id=None, department=None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")

        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN Disabilities ON Enrollment.PersonalID = Disabilities.PersonalID
        WHERE Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND ((Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType = 6 AND Disabilities.DisabilityResponse=1)
        OR (Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType=8 AND Disabilities.DisabilityResponse=1)
        OR (Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType IN (5,7,9,10) AND Disabilities.DisabilityResponse=1 AND Disabilities.IndefiniteAndImpairs=1))
        '''
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN Disabilities ON Enrollment.PersonalID = Disabilities.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND ((Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType = 6 AND Disabilities.DisabilityResponse=1)
        OR (Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType=8 AND Disabilities.DisabilityResponse=1)
        OR (Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType IN (5,7,9,10) AND Disabilities.DisabilityResponse=1 AND Disabilities.IndefiniteAndImpairs=1))
        '''

        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN Disabilities ON Enrollment.PersonalID = Disabilities.PersonalID
        WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
		AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND ((Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType = 6 AND Disabilities.DisabilityResponse=1)
        OR (Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType=8 AND Disabilities.DisabilityResponse=1)
        OR (Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType IN (5,7,9,10) AND Disabilities.DisabilityResponse=1 AND Disabilities.IndefiniteAndImpairs=1))
        '''          
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list  

class StartDate():
    def start_date_data_accuracy(self, start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None,server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")

        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                WHERE Enrollment.EntryDate <= %s 
                AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND (Enrollment.EntryDate < '1915-01-01'
                OR Enrollment.EntryDate > Exit.ExitDate)'''
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND Enrollment.EntryDate <= %s 
                AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND (Enrollment.EntryDate < '1915-01-01'
                OR Enrollment.EntryDate > Exit.ExitDate)'''
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
                AND Enrollment.DateOfEngagement <= %s
                AND Enrollment.EntryDate <= %s
                AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND (Enrollment.EntryDate < '1915-01-01'
                OR Enrollment.EntryDate > Exit.ExitDate)'''                
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """
        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list

class ExitDate():
    def exit_date_data_accuracy(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        sql_numerator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= ? THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND (Exit.ExitDate<Enrollment.EntryDate)"""
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.PersonalID=Enrollment.PersonalID
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND (Exit.ExitDate<Enrollment.EntryDate)'''
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.PersonalID=Enrollment.PersonalID
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
                AND Enrollment.DateOfEngagement <= ?
                AND Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND (Exit.ExitDate<Enrollment.EntryDate)'''
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= ? THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        c.execute(sql_numerator_combined, outreach_params)
        scores=c.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        c.execute(sql_denominator_combined, outreach_params)
        result=c.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        c.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = c.fetchall()
        
        c.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = c.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list


class HOH():
    def HoH_total_accuracy(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        sql_numerator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= %s 
                AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND ((Enrollment.RelationshipToHoH=99 OR Enrollment.RelationshipToHoH IS NULL) 
                OR 
                    (Enrollment.RelationshipToHoH != 1 AND NOT EXISTS (
                            SELECT 1 
                            FROM Enrollment 
                            WHERE Enrollment.HouseholdID = Enrollment.HouseholdID 
                            AND Enrollment.RelationshipToHoH = 1 
                        )) 
                    OR (Enrollment.RelationshipToHoH = 1 
                        AND EXISTS (
                            SELECT 1 
                            FROM Enrollment
                            WHERE Enrollment.HouseholdID = Enrollment.HouseholdID 
                            AND Enrollment.RelationshipToHoH = 1 
                            AND Enrollment.PersonalID != Enrollment.PersonalID
                        )))"""
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND ((Enrollment.RelationshipToHoH=99 OR Enrollment.RelationshipToHoH IS NULL) 
                OR 
                    (Enrollment.RelationshipToHoH != 1 AND NOT EXISTS (
                            SELECT 1 
                            FROM Enrollment 
                            WHERE Enrollment.HouseholdID = Enrollment.HouseholdID 
                            AND Enrollment.RelationshipToHoH = 1 
                        )) 
                    OR (Enrollment.RelationshipToHoH = 1 
                        AND EXISTS (
                            SELECT 1 
                            FROM Enrollment
                            WHERE Enrollment.HouseholdID = Enrollment.HouseholdID 
                            AND Enrollment.RelationshipToHoH = 1 
                            AND Enrollment.PersonalID != Enrollment.PersonalID
                        )))'''
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND ((Enrollment.RelationshipToHoH=99 OR Enrollment.RelationshipToHoH IS NULL) 
                OR 
                    (Enrollment.RelationshipToHoH != 1 AND NOT EXISTS (
                            SELECT 1 
                            FROM Enrollment 
                            WHERE Enrollment.HouseholdID = Enrollment.HouseholdID 
                            AND Enrollment.RelationshipToHoH = 1 
                        )) 
                    OR (Enrollment.RelationshipToHoH = 1 
                        AND EXISTS (
                            SELECT 1 
                            FROM Enrollment
                            WHERE Enrollment.HouseholdID = Enrollment.HouseholdID 
                            AND Enrollment.RelationshipToHoH = 1 
                            AND Enrollment.PersonalID != Enrollment.PersonalID
                        )))'''               
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list
        
    def HoH_missing(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        sql_numerator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= %s 
                AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND (Enrollment.RelationshipToHoH=99 OR Enrollment.RelationshipToHoH IS NULL)"""
        
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND Enrollment.EntryDate <= %s 
                AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND (Enrollment.RelationshipToHoH=99 OR Enrollment.RelationshipToHoH IS NULL)'''     
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
                AND Enrollment.DateOfEngagement <= %s
                AND Enrollment.EntryDate <= %s 
                AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND (Enrollment.RelationshipToHoH=99 OR Enrollment.RelationshipToHoH IS NULL)'''  
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list

    def HoH_data_accuracy(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
            conn=mysql.connector.connect(**server)
            cursor=conn.cursor()

            cursor.execute(f"USE `{db}`")
            
            sql_numerator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                    INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                    WHERE Enrollment.EntryDate <= %s 
                    AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                    AND (Enrollment.RelationshipToHoH != 1 AND NOT EXISTS (
                                SELECT 1 
                                FROM Enrollment 
                                WHERE Enrollment.HouseholdID = Enrollment.HouseholdID 
                                AND Enrollment.RelationshipToHoH = 1 
                            )) 
                        OR (Enrollment.RelationshipToHoH = 1 
                            AND EXISTS (
                                SELECT 1 
                                FROM Enrollment
                                WHERE Enrollment.HouseholdID = Enrollment.HouseholdID 
                                AND Enrollment.RelationshipToHoH = 1 
                                AND Enrollment.PersonalID != Enrollment.PersonalID
                            ))"""
            sql_non_outreach_list = '''
                SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                    FROM AdditionalInformation
                    LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                    LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                    INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                    WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
                    AND AdditionalInformation.StaffActiveStatus ='Yes'
                    AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                    AND Enrollment.EntryDate <= %s 
                    AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                    AND (Enrollment.RelationshipToHoH != 1 AND NOT EXISTS (
                                SELECT 1 
                                FROM Enrollment 
                                WHERE Enrollment.HouseholdID = Enrollment.HouseholdID 
                                AND Enrollment.RelationshipToHoH = 1 
                            )) 
                        OR (Enrollment.RelationshipToHoH = 1 
                            AND EXISTS (
                                SELECT 1 
                                FROM Enrollment
                                WHERE Enrollment.HouseholdID = Enrollment.HouseholdID 
                                AND Enrollment.RelationshipToHoH = 1 
                                AND Enrollment.PersonalID != Enrollment.PersonalID
                            ))'''   
            sql_outreach_list = '''
                SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                    FROM AdditionalInformation
                    LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                    LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                    INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                    WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
                    AND AdditionalInformation.StaffActiveStatus ='Yes'
                    AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
                    AND Enrollment.DateOfEngagement <= %s
                    AND Enrollment.EntryDate <= %s 
                    AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                    AND (Enrollment.RelationshipToHoH != 1 AND NOT EXISTS (
                                SELECT 1 
                                FROM Enrollment 
                                WHERE Enrollment.HouseholdID = Enrollment.HouseholdID 
                                AND Enrollment.RelationshipToHoH = 1 
                            )) 
                        OR (Enrollment.RelationshipToHoH = 1 
                            AND EXISTS (
                                SELECT 1 
                                FROM Enrollment
                                WHERE Enrollment.HouseholdID = Enrollment.HouseholdID 
                                AND Enrollment.RelationshipToHoH = 1 
                                AND Enrollment.PersonalID != Enrollment.PersonalID
                            ))'''                    
            sql_denominator_combined = """
            SELECT 
                COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
                COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
            FROM Enrollment
            LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE Enrollment.EntryDate <= %s 
            AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
            """

            filter_params = [end_date, start_date]
            outreach_params = [end_date, end_date, start_date]

            if program_id is not None:
                placeholders = ','.join(['%s' for _ in program_id])
                sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                filter_params.extend(program_id)
                outreach_params.extend(program_id)

            if department is not None:
                placeholders = ','.join(['%s' for _ in department])
                sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                filter_params.extend(department)
                outreach_params.extend(department)

            if region is not None:
                placeholders = ','.join(['%s' for _ in region])
                sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                filter_params.extend(region)
                outreach_params.extend(region)

            if program_type is not None:
                placeholders = ','.join(['%s' for _ in program_type])
                sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                filter_params.extend(program_type)
                outreach_params.extend(program_type)
                
            cursor.execute(sql_numerator_combined, outreach_params)
            scores=cursor.fetchone()
            non_outreach_score=scores[0]
            outreach_scores=scores[1]
            
            cursor.execute(sql_denominator_combined, outreach_params)
            result=cursor.fetchone()
            total_non_outreach = result[0]
            total_outreach=result[1]
            
            cursor.execute(sql_non_outreach_list, filter_params)
            non_outreach_pt_list = cursor.fetchall()
            
            cursor.execute(sql_outreach_list, outreach_params)
            outreach_pt_list = cursor.fetchall()
            
            conn.close()

            total_score=non_outreach_score+outreach_scores
            total_enrollments = total_outreach + total_non_outreach
            full_list=non_outreach_pt_list+outreach_pt_list
            if output==None:
                if isinstance(total_score, numbers.Number):                       
                    if total_enrollments > 0:
                        if 1-(total_score/total_enrollments) <= 0:
                            return 0
                        else:
                            return 1-(total_score/total_enrollments)
                    else:
                        return 0
                else:
                    return None
            elif output =="list":
                return full_list


class Location():
    def enrollment_coc_total_accuracy(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None,server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= %s 
                AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND  ((Enrollment.RelationshipToHoH = 1 AND Enrollment.EnrollmentCoC IS NULL) 
                OR (Enrollment.RelationshipToHoH = 1 AND Enrollment.EnrollmentCoC IS NOT NULL AND Enrollment.EnrollmentCoC NOT IN 
                        ('CA-600', 'CA-601', 'CA-606', 'CA-602', 'CA-500', 'CA-612', 'CA-614', 'CA-607', 'CA-603')))'''
                        
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND Enrollment.EntryDate <= %s 
                AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND  ((Enrollment.RelationshipToHoH = 1 AND Enrollment.EnrollmentCoC IS NULL) 
                OR (Enrollment.RelationshipToHoH = 1 AND Enrollment.EnrollmentCoC IS NOT NULL AND Enrollment.EnrollmentCoC NOT IN 
                        ('CA-600', 'CA-601', 'CA-606', 'CA-602', 'CA-500', 'CA-612', 'CA-614', 'CA-607','CA-603')))'''
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
                AND Enrollment.DateOfEngagement <= %s
                AND Enrollment.EntryDate <= %s 
                AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND  ((Enrollment.RelationshipToHoH = 1 AND Enrollment.EnrollmentCoC IS NULL) 
                OR (Enrollment.RelationshipToHoH = 1 AND Enrollment.EnrollmentCoC IS NOT NULL AND Enrollment.EnrollmentCoC NOT IN 
                        ('CA-600', 'CA-601', 'CA-606', 'CA-602', 'CA-500', 'CA-612', 'CA-614', 'CA-607','CA-603')))'''                
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list
        
    def enrollment_coc_missing(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= %s 
                AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND Enrollment.RelationshipToHoH = 1 
                AND Enrollment.EnrollmentCoC IS NULL'''
                
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND Enrollment.EntryDate <= %s 
                AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND Enrollment.RelationshipToHoH = 1 
                AND Enrollment.EnrollmentCoC IS NULL'''
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
                AND Enrollment.DateOfEngagement <= %s
                AND Enrollment.EntryDate <= %s 
                AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND Enrollment.RelationshipToHoH = 1 
                AND Enrollment.EnrollmentCoC IS NULL'''             
                
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list
        
    def enrollment_coc_data_accuracy(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
            conn=mysql.connector.connect(**server)
            cursor=conn.cursor()

            cursor.execute(f"USE `{db}`")
            
            sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                    INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                    WHERE Enrollment.EntryDate <= %s 
                    AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                    AND Enrollment.RelationshipToHoH = 1 
                    AND Enrollment.EnrollmentCoC IS NOT NULL 
                    AND Enrollment.EnrollmentCoC NOT IN 
                            ('CA-600', 'CA-601', 'CA-606', 'CA-602', 'CA-500', 'CA-612', 'CA-614', 'CA-607','CA-603')'''
            sql_non_outreach_list = '''
                SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                    FROM AdditionalInformation
                    LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                    LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                    INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                    WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
                    AND AdditionalInformation.StaffActiveStatus ='Yes'
                    AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                    AND Enrollment.EntryDate <= %s 
                    AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                    AND Enrollment.RelationshipToHoH = 1 
                    AND Enrollment.EnrollmentCoC IS NOT NULL 
                    AND Enrollment.EnrollmentCoC NOT IN 
                            ('CA-600', 'CA-601', 'CA-606', 'CA-602', 'CA-500', 'CA-612', 'CA-614', 'CA-607','CA-603')'''
            sql_outreach_list = '''
                SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                    FROM AdditionalInformation
                    LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                    LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                    INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                    WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
                    AND AdditionalInformation.StaffActiveStatus ='Yes'
                    AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
                    AND Enrollment.DateOfEngagement <= %s
                    AND Enrollment.EntryDate <= %s
                    AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                    AND Enrollment.RelationshipToHoH = 1 
                    AND Enrollment.EnrollmentCoC IS NOT NULL 
                    AND Enrollment.EnrollmentCoC NOT IN 
                            ('CA-600', 'CA-601', 'CA-606', 'CA-602', 'CA-500', 'CA-612', 'CA-614', 'CA-607','CA-603')'''                     
            sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        """

            filter_params = [end_date, start_date]
            outreach_params = [end_date, end_date, start_date]

            if program_id is not None:
                placeholders = ','.join(['%s' for _ in program_id])
                sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                filter_params.extend(program_id)
                outreach_params.extend(program_id)

            if department is not None:
                placeholders = ','.join(['%s' for _ in department])
                sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                filter_params.extend(department)
                outreach_params.extend(department)

            if region is not None:
                placeholders = ','.join(['%s' for _ in region])
                sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                filter_params.extend(region)
                outreach_params.extend(region)

            if program_type is not None:
                placeholders = ','.join(['%d' for _ in program_type])
                sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                filter_params.extend(program_type)
                outreach_params.extend(program_type)
                
            cursor.execute(sql_numerator_combined, outreach_params)
            scores=cursor.fetchone()
            non_outreach_score=scores[0]
            outreach_scores=scores[1]
            
            cursor.execute(sql_denominator_combined, outreach_params)
            result=cursor.fetchone()
            total_non_outreach = result[0]
            total_outreach=result[1]
            
            cursor.execute(sql_non_outreach_list, filter_params)
            non_outreach_pt_list = cursor.fetchall()
            
            cursor.execute(sql_outreach_list, outreach_params)
            outreach_pt_list = cursor.fetchall()
            
            conn.close()

            total_score=non_outreach_score+outreach_scores
            total_enrollments = total_outreach + total_non_outreach
            full_list=non_outreach_pt_list+outreach_pt_list
            if output==None:
                if isinstance(total_score, numbers.Number):                       
                    if total_enrollments > 0:
                        if 1-(total_score/total_enrollments) <= 0:
                            return 0
                        else:
                            return 1-(total_score/total_enrollments)
                    else:
                        return 0
                else:
                    return None
            elif output =="list":
                return full_list

class Destination():
    def destination_total_accuracy(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        #this should include just leavers
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        sql_numerator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
                AND  (Exit.Destination IN (8,9) OR (Exit.Destination=30 OR Exit.Destination IS NULL OR Exit.Destination =99)) """
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
                AND  (Exit.Destination IN (8,9) OR (Exit.Destination=30 OR Exit.Destination IS NULL OR Exit.Destination =99)) '''
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
                AND Enrollment.DateOfEngagement <= %s
                AND (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
                AND  (Exit.Destination IN (8,9) OR (Exit.Destination=30 OR Exit.Destination IS NULL OR Exit.Destination =99)) '''
             
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
        """


        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list
    
    def destination_client_refused_doesnt_know(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        #this should include just leavers
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        sql_numerator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
                AND  (Exit.Destination IN (8,9))"""
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
                AND  (Exit.Destination IN (8,9))'''
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
                AND Enrollment.DateOfEngagement <= %s
                AND (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
                AND  (Exit.Destination IN (8,9))'''                
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list

    def destination_missing(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None,server=server_config,db=database_name):
        #this should include just leavers
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        sql_numerator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
                AND (Exit.Destination=30 OR Exit.Destination IS NULL OR Exit.Destination =99)"""
                
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
                AND (Exit.Destination=30 OR Exit.Destination IS NULL OR Exit.Destination =99)'''
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
                AND Enrollment.DateOfEngagement <= %s
                AND (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
                AND (Exit.Destination=30 OR Exit.Destination IS NULL OR Exit.Destination =99)'''
                
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list
        
    def number_leavers(self,start_date, end_date,program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        #this should include just leavers
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
    
                
        sql_numerator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
        """


        filter_params = [end_date,end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)


        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)


        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
        
        cursor.execute(sql_numerator_combined, filter_params)
        result=cursor.fetchone()
        total_non_outreach_exits = result[0]
        total_outreach_exits=result[1]
        

        conn.close()
        total_exits = total_outreach_exits + total_non_outreach_exits

        return total_exits

class Income():
    def starting_income_total_accuracy(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None,server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
    INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
		INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
    WHERE (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH = 1)
        AND (Enrollment.EntryDate <= %s)
        AND (Exit.ExitDate >=%s OR Exit.ExitDate IS NULL)
        AND (
            (
                Enrollment.EntryDate = IncomeBenefits.InformationDate
                AND (IncomeBenefits.DataCollectionStage = 1)
                AND (
                    IncomeBenefits.IncomeFromAnySource IN (8,9)
                )
            )
            OR (
                Enrollment.EntryDate = IncomeBenefits.InformationDate
                AND (IncomeBenefits.DataCollectionStage = 1)
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 1 AND (
                            IncomeBenefits.Earned IS NULL OR IncomeBenefits.Unemployment IS NULL OR IncomeBenefits.SSI IS NULL 
                            OR IncomeBenefits.SSDI IS NULL OR IncomeBenefits.VADisabilityService IS NULL
                            OR IncomeBenefits.VADisabilityNonService IS NULL OR IncomeBenefits.PrivateDisability IS NULL 
                            OR IncomeBenefits.WorkersComp IS NULL OR IncomeBenefits.TANF IS NULL
                            OR IncomeBenefits.GA IS NULL OR IncomeBenefits.SocSecRetirement IS NULL 
                            OR IncomeBenefits.Pension IS NULL OR IncomeBenefits.ChildSupport IS NULL 
                            OR IncomeBenefits.Alimony IS NULL OR IncomeBenefits.OtherIncomeSource IS NULL
                        )
                    )
                    OR (IncomeBenefits.IncomeFromAnySource = 99 or IncomeBenefits.IncomeFromAnySource is NULL)
                )
            )
            OR (
                Enrollment.EntryDate != IncomeBenefits.InformationDate
                AND (IncomeBenefits.DataCollectionStage = 1)
            )
            OR (
                Enrollment.EntryDate = IncomeBenefits.InformationDate
                AND (IncomeBenefits.DataCollectionStage = 1)
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 1)
                    AND IncomeBenefits.Earned = 0
                    AND IncomeBenefits.Unemployment = 0
                    AND IncomeBenefits.SSI = 0
                    AND IncomeBenefits.SSDI = 0
                    AND IncomeBenefits.VADisabilityService = 0
                    AND IncomeBenefits.VADisabilityNonService = 0
                    AND IncomeBenefits.PrivateDisability = 0
                    AND IncomeBenefits.WorkersComp = 0
                    AND IncomeBenefits.TANF = 0
                    AND IncomeBenefits.GA = 0
                    AND IncomeBenefits.SocSecRetirement = 0
                    AND IncomeBenefits.Pension = 0
                    AND IncomeBenefits.ChildSupport = 0
                    AND IncomeBenefits.Alimony = 0
                    AND IncomeBenefits.OtherIncomeSource = 0
                )
            )
    )
        '''

        sql_non_outreach_list = '''
                SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                From AdditionalInformation
                LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
    INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
    WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
    AND AdditionalInformation.StaffActiveStatus ='Yes'
    AND (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH = 1)
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND (
            (
                Enrollment.EntryDate = IncomeBenefits.InformationDate
                AND (IncomeBenefits.DataCollectionStage = 1)
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 8 OR IncomeBenefits.IncomeFromAnySource = 9)
                )
            )
            OR (
                Enrollment.EntryDate = IncomeBenefits.InformationDate
                AND (IncomeBenefits.DataCollectionStage = 1)
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 1 AND (
                            IncomeBenefits.Earned IS NULL OR IncomeBenefits.Unemployment IS NULL OR IncomeBenefits.SSI IS NULL 
                            OR IncomeBenefits.SSDI IS NULL OR IncomeBenefits.VADisabilityService IS NULL
                            OR IncomeBenefits.VADisabilityNonService IS NULL OR IncomeBenefits.PrivateDisability IS NULL 
                            OR IncomeBenefits.WorkersComp IS NULL OR IncomeBenefits.TANF IS NULL
                            OR IncomeBenefits.GA IS NULL OR IncomeBenefits.SocSecRetirement IS NULL 
                            OR IncomeBenefits.Pension IS NULL OR IncomeBenefits.ChildSupport IS NULL 
                            OR IncomeBenefits.Alimony IS NULL OR IncomeBenefits.OtherIncomeSource IS NULL
                        )
                    )
                    OR (IncomeBenefits.IncomeFromAnySource = 99 or IncomeBenefits.IncomeFromAnySource is NULL)
                )
            )
            OR (
                Enrollment.EntryDate != IncomeBenefits.InformationDate
                AND (IncomeBenefits.DataCollectionStage = 1)
            )
            OR (
                Enrollment.EntryDate = IncomeBenefits.InformationDate
                AND (IncomeBenefits.DataCollectionStage = 1)
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 1)
                    AND IncomeBenefits.Earned = 0
                    AND IncomeBenefits.Unemployment = 0
                    AND IncomeBenefits.SSI = 0
                    AND IncomeBenefits.SSDI = 0
                    AND IncomeBenefits.VADisabilityService = 0
                    AND IncomeBenefits.VADisabilityNonService = 0
                    AND IncomeBenefits.PrivateDisability = 0
                    AND IncomeBenefits.WorkersComp = 0
                    AND IncomeBenefits.TANF = 0
                    AND IncomeBenefits.GA = 0
                    AND IncomeBenefits.SocSecRetirement = 0
                    AND IncomeBenefits.Pension = 0
                    AND IncomeBenefits.ChildSupport = 0
                    AND IncomeBenefits.Alimony = 0
                    AND IncomeBenefits.OtherIncomeSource = 0
                )
            )
    )
        '''
        sql_outreach_list = '''
                SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                From AdditionalInformation
                LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
    INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
    WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
    AND AdditionalInformation.StaffActiveStatus ='Yes'
    AND (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH = 1)
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
        AND (
            (
                Enrollment.EntryDate = IncomeBenefits.InformationDate
                AND (IncomeBenefits.DataCollectionStage = 1)
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 8 OR IncomeBenefits.IncomeFromAnySource = 9)
                )
            )
            OR (
                Enrollment.EntryDate = IncomeBenefits.InformationDate
                AND (IncomeBenefits.DataCollectionStage = 1)
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 1 AND (
                            IncomeBenefits.Earned IS NULL OR IncomeBenefits.Unemployment IS NULL OR IncomeBenefits.SSI IS NULL 
                            OR IncomeBenefits.SSDI IS NULL OR IncomeBenefits.VADisabilityService IS NULL
                            OR IncomeBenefits.VADisabilityNonService IS NULL OR IncomeBenefits.PrivateDisability IS NULL 
                            OR IncomeBenefits.WorkersComp IS NULL OR IncomeBenefits.TANF IS NULL
                            OR IncomeBenefits.GA IS NULL OR IncomeBenefits.SocSecRetirement IS NULL 
                            OR IncomeBenefits.Pension IS NULL OR IncomeBenefits.ChildSupport IS NULL 
                            OR IncomeBenefits.Alimony IS NULL OR IncomeBenefits.OtherIncomeSource IS NULL
                        )
                    )
                    OR (IncomeBenefits.IncomeFromAnySource = 99 or IncomeBenefits.IncomeFromAnySource is NULL)
                )
            )
            OR (
                Enrollment.EntryDate != IncomeBenefits.InformationDate
                AND (IncomeBenefits.DataCollectionStage = 1)
            )
            OR (
                Enrollment.EntryDate = IncomeBenefits.InformationDate
                AND (IncomeBenefits.DataCollectionStage = 1)
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 1)
                    AND IncomeBenefits.Earned = 0
                    AND IncomeBenefits.Unemployment = 0
                    AND IncomeBenefits.SSI = 0
                    AND IncomeBenefits.SSDI = 0
                    AND IncomeBenefits.VADisabilityService = 0
                    AND IncomeBenefits.VADisabilityNonService = 0
                    AND IncomeBenefits.PrivateDisability = 0
                    AND IncomeBenefits.WorkersComp = 0
                    AND IncomeBenefits.TANF = 0
                    AND IncomeBenefits.GA = 0
                    AND IncomeBenefits.SocSecRetirement = 0
                    AND IncomeBenefits.Pension = 0
                    AND IncomeBenefits.ChildSupport = 0
                    AND IncomeBenefits.Alimony = 0
                    AND IncomeBenefits.OtherIncomeSource = 0
                )
            )
    )
        '''                
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                        AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list
    
        
    def starting_income_client_refused(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
		INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
                WHERE (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH=1)
                AND (Enrollment.EntryDate <= %s)
                AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND (Enrollment.EntryDate =IncomeBenefits.InformationDate)
                AND (IncomeBenefits.DataCollectionStage=1)
                AND (IncomeBenefits.IncomeFromAnySource IN (8,9))             
            '''
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH=1)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND Enrollment.EntryDate <= %s 
                AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND (Enrollment.EntryDate =IncomeBenefits.InformationDate)
                AND (IncomeBenefits.DataCollectionStage=1)
                AND (IncomeBenefits.IncomeFromAnySource IN (8,9))   '''
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH=1)
                AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
                AND Enrollment.DateOfEngagement <= %s
                AND Enrollment.EntryDate <= %s 
                AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND (Enrollment.EntryDate =IncomeBenefits.InformationDate)
                AND (IncomeBenefits.DataCollectionStage=1)
                AND (IncomeBenefits.IncomeFromAnySource IN (8,9))   '''                
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                        AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list

    
    def starting_income_missing(self,start_date, end_date,  output=None,program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
        WHERE  (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH = 1)
            AND (Enrollment.EntryDate <= %s)
            AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
            AND (
                (
                    Enrollment.EntryDate = IncomeBenefits.InformationDate
                    AND (IncomeBenefits.DataCollectionStage = 1)
                    AND (
                        (IncomeBenefits.IncomeFromAnySource = 1 AND (
                                IncomeBenefits.Earned IS NULL OR IncomeBenefits.Unemployment IS NULL OR IncomeBenefits.SSI IS NULL 
                                OR IncomeBenefits.SSDI IS NULL OR IncomeBenefits.VADisabilityService IS NULL
                                OR IncomeBenefits.VADisabilityNonService IS NULL OR IncomeBenefits.PrivateDisability IS NULL 
                                OR IncomeBenefits.WorkersComp IS NULL OR IncomeBenefits.TANF IS NULL
                                OR IncomeBenefits.GA IS NULL OR IncomeBenefits.SocSecRetirement IS NULL 
                                OR IncomeBenefits.Pension IS NULL OR IncomeBenefits.ChildSupport IS NULL 
                                OR IncomeBenefits.Alimony IS NULL OR IncomeBenefits.OtherIncomeSource IS NULL
                            )
                        )
                        OR (IncomeBenefits.IncomeFromAnySource=99 or IncomeBenefits.IncomeFromAnySource is NULL)
                    )
                )
                OR (
                    Enrollment.EntryDate != IncomeBenefits.InformationDate
                    AND (IncomeBenefits.DataCollectionStage = 1)
                )
            )
                
        '''
        
        sql_non_outreach_list = '''
                SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                From AdditionalInformation
                LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
    INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
    WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
    AND AdditionalInformation.StaffActiveStatus ='Yes'
    AND (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH = 1)
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
            AND (
                (
                    Enrollment.EntryDate = IncomeBenefits.InformationDate
                    AND (IncomeBenefits.DataCollectionStage = 1)
                    AND (
                        (IncomeBenefits.IncomeFromAnySource = 1 AND (
                                IncomeBenefits.Earned IS NULL OR IncomeBenefits.Unemployment IS NULL OR IncomeBenefits.SSI IS NULL 
                                OR IncomeBenefits.SSDI IS NULL OR IncomeBenefits.VADisabilityService IS NULL
                                OR IncomeBenefits.VADisabilityNonService IS NULL OR IncomeBenefits.PrivateDisability IS NULL 
                                OR IncomeBenefits.WorkersComp IS NULL OR IncomeBenefits.TANF IS NULL
                                OR IncomeBenefits.GA IS NULL OR IncomeBenefits.SocSecRetirement IS NULL 
                                OR IncomeBenefits.Pension IS NULL OR IncomeBenefits.ChildSupport IS NULL 
                                OR IncomeBenefits.Alimony IS NULL OR IncomeBenefits.OtherIncomeSource IS NULL
                            )
                        )
                        OR (IncomeBenefits.IncomeFromAnySource=99 or IncomeBenefits.IncomeFromAnySource is NULL)
                    )
                )
                OR (
                    Enrollment.EntryDate != IncomeBenefits.InformationDate
                    AND (IncomeBenefits.DataCollectionStage = 1)
                )
            )
                
        '''
        sql_outreach_list = '''
                SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                From AdditionalInformation
                LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
    INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
    WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
    AND AdditionalInformation.StaffActiveStatus ='Yes'
    AND (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH = 1)
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
            AND (
                (
                    Enrollment.EntryDate = IncomeBenefits.InformationDate
                    AND (IncomeBenefits.DataCollectionStage = 1)
                    AND (
                        (IncomeBenefits.IncomeFromAnySource = 1 AND (
                                IncomeBenefits.Earned IS NULL OR IncomeBenefits.Unemployment IS NULL OR IncomeBenefits.SSI IS NULL 
                                OR IncomeBenefits.SSDI IS NULL OR IncomeBenefits.VADisabilityService IS NULL
                                OR IncomeBenefits.VADisabilityNonService IS NULL OR IncomeBenefits.PrivateDisability IS NULL 
                                OR IncomeBenefits.WorkersComp IS NULL OR IncomeBenefits.TANF IS NULL
                                OR IncomeBenefits.GA IS NULL OR IncomeBenefits.SocSecRetirement IS NULL 
                                OR IncomeBenefits.Pension IS NULL OR IncomeBenefits.ChildSupport IS NULL 
                                OR IncomeBenefits.Alimony IS NULL OR IncomeBenefits.OtherIncomeSource IS NULL
                            )
                        )
                        OR (IncomeBenefits.IncomeFromAnySource=99 or IncomeBenefits.IncomeFromAnySource is NULL)
                    )
                )
                OR (
                    Enrollment.EntryDate != IncomeBenefits.InformationDate
                    AND (IncomeBenefits.DataCollectionStage = 1)
                )
            )
                
        '''                
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                        AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list

    def starting_income_data_accuracy(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
            WHERE
                (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH = 1)
                AND (Enrollment.EntryDate <= %s)
                AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND (Enrollment.EntryDate =IncomeBenefits.InformationDate)
				AND (IncomeBenefits.DataCollectionStage = 1)
				AND ((IncomeBenefits.IncomeFromAnySource=1)
                    AND IncomeBenefits.Earned = 0
                    AND IncomeBenefits.Unemployment = 0
                    AND IncomeBenefits.SSI = 0
                    AND IncomeBenefits.SSDI = 0
                    AND IncomeBenefits.VADisabilityService = 0
                    AND IncomeBenefits.VADisabilityNonService = 0
                    AND IncomeBenefits.PrivateDisability = 0
                    AND IncomeBenefits.WorkersComp = 0
                    AND IncomeBenefits.TANF = 0
                    AND IncomeBenefits.GA = 0
                    AND IncomeBenefits.SocSecRetirement = 0
                    AND IncomeBenefits.Pension = 0
                    AND IncomeBenefits.ChildSupport = 0
                    AND IncomeBenefits.Alimony = 0
                    AND IncomeBenefits.OtherIncomeSource = 0)
                
        '''
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH = 1)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND Enrollment.EntryDate <= %s 
                AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND (Enrollment.EntryDate =IncomeBenefits.InformationDate)
				AND (IncomeBenefits.DataCollectionStage = 1)
				AND ((IncomeBenefits.IncomeFromAnySource=1)
                    AND IncomeBenefits.Earned = 0
                    AND IncomeBenefits.Unemployment = 0
                    AND IncomeBenefits.SSI = 0
                    AND IncomeBenefits.SSDI = 0
                    AND IncomeBenefits.VADisabilityService = 0
                    AND IncomeBenefits.VADisabilityNonService = 0
                    AND IncomeBenefits.PrivateDisability = 0
                    AND IncomeBenefits.WorkersComp = 0
                    AND IncomeBenefits.TANF = 0
                    AND IncomeBenefits.GA = 0
                    AND IncomeBenefits.SocSecRetirement = 0
                    AND IncomeBenefits.Pension = 0
                    AND IncomeBenefits.ChildSupport = 0
                    AND IncomeBenefits.Alimony = 0
                    AND IncomeBenefits.OtherIncomeSource = 0)'''
                
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH = 1)
            AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
            AND Enrollment.DateOfEngagement <= %s
            AND Enrollment.EntryDate <= %s 
            AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                AND (Enrollment.EntryDate =IncomeBenefits.InformationDate)
				AND (IncomeBenefits.DataCollectionStage = 1)
				AND ((IncomeBenefits.IncomeFromAnySource=1)
                    AND IncomeBenefits.Earned = 0
                    AND IncomeBenefits.Unemployment = 0
                    AND IncomeBenefits.SSI = 0
                    AND IncomeBenefits.SSDI = 0
                    AND IncomeBenefits.VADisabilityService = 0
                    AND IncomeBenefits.VADisabilityNonService = 0
                    AND IncomeBenefits.PrivateDisability = 0
                    AND IncomeBenefits.WorkersComp = 0
                    AND IncomeBenefits.TANF = 0
                    AND IncomeBenefits.GA = 0
                    AND IncomeBenefits.SocSecRetirement = 0
                    AND IncomeBenefits.Pension = 0
                    AND IncomeBenefits.ChildSupport = 0
                    AND IncomeBenefits.Alimony = 0
                    AND IncomeBenefits.OtherIncomeSource = 0)'''
                                    
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE Enrollment.EntryDate <= %s 
        AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
                        AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list
        
    #stayers
    def annual_income_total_accuracy(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
    INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
    WHERE (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH = 1)
          AND (Enrollment.EntryDate <= %s) 
        AND (
            (Exit.ExitDate >= DATE_ADD(Enrollment.EntryDate, INTERVAL 1 YEAR) AND Exit.ExitDate > %s)
            OR (Exit.ExitDate IS NULL AND DATEDIFF(CURDATE(), Enrollment.EntryDate) >= 365)
        )
          AND (
            (
            IncomeBenefits.DataCollectionStage = 5
        AND (
            IncomeBenefits.InformationDate BETWEEN DATE_SUB(Enrollment.EntryDate, INTERVAL 30 DAY) 
            AND DATE_ADD(Enrollment.EntryDate, INTERVAL 30 DAY)
        )
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 8 OR IncomeBenefits.IncomeFromAnySource = 9)
                )
            )
            OR (
            (IncomeBenefits.DataCollectionStage = 5)
        AND (
            IncomeBenefits.InformationDate BETWEEN DATE_SUB(Enrollment.EntryDate, INTERVAL 30 DAY) 
            AND DATE_ADD(Enrollment.EntryDate, INTERVAL 30 DAY)
        )
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 1 AND (
                            IncomeBenefits.Earned IS NULL OR IncomeBenefits.Unemployment IS NULL OR IncomeBenefits.SSI IS NULL 
                            OR IncomeBenefits.SSDI IS NULL OR IncomeBenefits.VADisabilityService IS NULL
                            OR IncomeBenefits.VADisabilityNonService IS NULL OR IncomeBenefits.PrivateDisability IS NULL 
                            OR IncomeBenefits.WorkersComp IS NULL OR IncomeBenefits.TANF IS NULL
                            OR IncomeBenefits.GA IS NULL OR IncomeBenefits.SocSecRetirement IS NULL 
                            OR IncomeBenefits.Pension IS NULL OR IncomeBenefits.ChildSupport IS NULL 
                            OR IncomeBenefits.Alimony IS NULL OR IncomeBenefits.OtherIncomeSource IS NULL
                        )
                    )
                    OR (IncomeBenefits.IncomeFromAnySource = 99 or IncomeBenefits.IncomeFromAnySource is NULL)
                )
            )
            OR (
            (IncomeBenefits.DataCollectionStage = 5)
        AND (
            IncomeBenefits.InformationDate BETWEEN DATE_SUB(Enrollment.EntryDate, INTERVAL 30 DAY) 
            AND DATE_ADD(Enrollment.EntryDate, INTERVAL 30 DAY)
        )
            )
            OR (
        (IncomeBenefits.DataCollectionStage = 5)
        AND (
            IncomeBenefits.InformationDate BETWEEN DATE_SUB(Enrollment.EntryDate, INTERVAL 30 DAY) 
            AND DATE_ADD(Enrollment.EntryDate, INTERVAL 30 DAY)
        )
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 1)
                    AND IncomeBenefits.Earned = 0
                    AND IncomeBenefits.Unemployment = 0
                    AND IncomeBenefits.SSI = 0
                    AND IncomeBenefits.SSDI = 0
                    AND IncomeBenefits.VADisabilityService = 0
                    AND IncomeBenefits.VADisabilityNonService = 0
                    AND IncomeBenefits.PrivateDisability = 0
                    AND IncomeBenefits.WorkersComp = 0
                    AND IncomeBenefits.TANF = 0
                    AND IncomeBenefits.GA = 0
                    AND IncomeBenefits.SocSecRetirement = 0
                    AND IncomeBenefits.Pension = 0
                    AND IncomeBenefits.ChildSupport = 0
                    AND IncomeBenefits.Alimony = 0
                    AND IncomeBenefits.OtherIncomeSource = 0
                )
            )
    )'''

        sql_non_outreach_list = '''
    SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
    FROM AdditionalInformation
	LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
    LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
    INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
    INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
    WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
	AND AdditionalInformation.StaffActiveStatus ='Yes'
    AND (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH = 1)
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (
            (Exit.ExitDate >= DATE_ADD(Enrollment.EntryDate, INTERVAL 1 YEAR) AND Exit.ExitDate > %s)
            OR (Exit.ExitDate IS NULL AND DATEDIFF(CURDATE(), Enrollment.EntryDate) >= 365)
        )
          AND (
            (
            IncomeBenefits.DataCollectionStage = 5
        AND (
            IncomeBenefits.InformationDate BETWEEN DATE_SUB(Enrollment.EntryDate, INTERVAL 30 DAY) 
            AND DATE_ADD(Enrollment.EntryDate, INTERVAL 30 DAY)
        )
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 8 OR IncomeBenefits.IncomeFromAnySource = 9)
                )
            )
            OR (
            (IncomeBenefits.DataCollectionStage = 5)
        AND (
            IncomeBenefits.InformationDate BETWEEN DATE_SUB(Enrollment.EntryDate, INTERVAL 30 DAY) 
            AND DATE_ADD(Enrollment.EntryDate, INTERVAL 30 DAY)
        )
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 1 AND (
                            IncomeBenefits.Earned IS NULL OR IncomeBenefits.Unemployment IS NULL OR IncomeBenefits.SSI IS NULL 
                            OR IncomeBenefits.SSDI IS NULL OR IncomeBenefits.VADisabilityService IS NULL
                            OR IncomeBenefits.VADisabilityNonService IS NULL OR IncomeBenefits.PrivateDisability IS NULL 
                            OR IncomeBenefits.WorkersComp IS NULL OR IncomeBenefits.TANF IS NULL
                            OR IncomeBenefits.GA IS NULL OR IncomeBenefits.SocSecRetirement IS NULL 
                            OR IncomeBenefits.Pension IS NULL OR IncomeBenefits.ChildSupport IS NULL 
                            OR IncomeBenefits.Alimony IS NULL OR IncomeBenefits.OtherIncomeSource IS NULL
                        )
                    )
                    OR (IncomeBenefits.IncomeFromAnySource = 99 or IncomeBenefits.IncomeFromAnySource is NULL)
                )
            )
            OR (
            (IncomeBenefits.DataCollectionStage = 5)
        AND (
            IncomeBenefits.InformationDate BETWEEN DATE_SUB(Enrollment.EntryDate, INTERVAL 30 DAY) 
            AND DATE_ADD(Enrollment.EntryDate, INTERVAL 30 DAY)
        )
            )
            OR (
        (IncomeBenefits.DataCollectionStage = 5)
        AND (
            IncomeBenefits.InformationDate BETWEEN DATE_SUB(Enrollment.EntryDate, INTERVAL 30 DAY) 
            AND DATE_ADD(Enrollment.EntryDate, INTERVAL 30 DAY)
        )
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 1)
                    AND IncomeBenefits.Earned = 0
                    AND IncomeBenefits.Unemployment = 0
                    AND IncomeBenefits.SSI = 0
                    AND IncomeBenefits.SSDI = 0
                    AND IncomeBenefits.VADisabilityService = 0
                    AND IncomeBenefits.VADisabilityNonService = 0
                    AND IncomeBenefits.PrivateDisability = 0
                    AND IncomeBenefits.WorkersComp = 0
                    AND IncomeBenefits.TANF = 0
                    AND IncomeBenefits.GA = 0
                    AND IncomeBenefits.SocSecRetirement = 0
                    AND IncomeBenefits.Pension = 0
                    AND IncomeBenefits.ChildSupport = 0
                    AND IncomeBenefits.Alimony = 0
                    AND IncomeBenefits.OtherIncomeSource = 0
                )
            )
    )
        '''
        sql_outreach_list = '''
    SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
    FROM AdditionalInformation
	LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
    LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
    INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
    INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
    WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
	AND AdditionalInformation.StaffActiveStatus ='Yes'
    AND (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH = 1)
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s 
        AND (
            (Exit.ExitDate >= DATE_ADD(Enrollment.EntryDate, INTERVAL 1 YEAR) AND Exit.ExitDate > %s)
            OR (Exit.ExitDate IS NULL AND DATEDIFF(CURDATE(), Enrollment.EntryDate) >= 365)
        )
          AND (
            (
            IncomeBenefits.DataCollectionStage = 5
        AND (
            IncomeBenefits.InformationDate BETWEEN DATE_SUB(Enrollment.EntryDate, INTERVAL 30 DAY) 
            AND DATE_ADD(Enrollment.EntryDate, INTERVAL 30 DAY)
        )
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 8 OR IncomeBenefits.IncomeFromAnySource = 9)
                )
            )
            OR (
            (IncomeBenefits.DataCollectionStage = 5)
        AND (
            IncomeBenefits.InformationDate BETWEEN DATE_SUB(Enrollment.EntryDate, INTERVAL 30 DAY) 
            AND DATE_ADD(Enrollment.EntryDate, INTERVAL 30 DAY)
        )
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 1 AND (
                            IncomeBenefits.Earned IS NULL OR IncomeBenefits.Unemployment IS NULL OR IncomeBenefits.SSI IS NULL 
                            OR IncomeBenefits.SSDI IS NULL OR IncomeBenefits.VADisabilityService IS NULL
                            OR IncomeBenefits.VADisabilityNonService IS NULL OR IncomeBenefits.PrivateDisability IS NULL 
                            OR IncomeBenefits.WorkersComp IS NULL OR IncomeBenefits.TANF IS NULL
                            OR IncomeBenefits.GA IS NULL OR IncomeBenefits.SocSecRetirement IS NULL 
                            OR IncomeBenefits.Pension IS NULL OR IncomeBenefits.ChildSupport IS NULL 
                            OR IncomeBenefits.Alimony IS NULL OR IncomeBenefits.OtherIncomeSource IS NULL
                        )
                    )
                    OR (IncomeBenefits.IncomeFromAnySource = 99 or IncomeBenefits.IncomeFromAnySource is NULL)
                )
            )
            OR (
            (IncomeBenefits.DataCollectionStage = 5)
        AND (
            IncomeBenefits.InformationDate BETWEEN DATE_SUB(Enrollment.EntryDate, INTERVAL 30 DAY) 
            AND DATE_ADD(Enrollment.EntryDate, INTERVAL 30 DAY)
        )
            )
            OR (
        (IncomeBenefits.DataCollectionStage = 5)
        AND (
            IncomeBenefits.InformationDate BETWEEN DATE_SUB(Enrollment.EntryDate, INTERVAL 30 DAY) 
            AND DATE_ADD(Enrollment.EntryDate, INTERVAL 30 DAY)
        )
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 1)
                    AND IncomeBenefits.Earned = 0
                    AND IncomeBenefits.Unemployment = 0
                    AND IncomeBenefits.SSI = 0
                    AND IncomeBenefits.SSDI = 0
                    AND IncomeBenefits.VADisabilityService = 0
                    AND IncomeBenefits.VADisabilityNonService = 0
                    AND IncomeBenefits.PrivateDisability = 0
                    AND IncomeBenefits.WorkersComp = 0
                    AND IncomeBenefits.TANF = 0
                    AND IncomeBenefits.GA = 0
                    AND IncomeBenefits.SocSecRetirement = 0
                    AND IncomeBenefits.Pension = 0
                    AND IncomeBenefits.ChildSupport = 0
                    AND IncomeBenefits.Alimony = 0
                    AND IncomeBenefits.OtherIncomeSource = 0
                )
            )
    )
        '''                
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE Enrollment.EntryDate <= %s 
        AND (
            (Exit.ExitDate >= DATE_ADD(Enrollment.EntryDate, INTERVAL 1 YEAR) AND Exit.ExitDate > %s)
            OR (Exit.ExitDate IS NULL AND DATEDIFF(CURDATE(), Enrollment.EntryDate) >= 365)
        )
        AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18
        """
        filter_params = [end_date, end_date]
        outreach_params = [end_date, end_date, end_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list

    #stayers
    def annual_income_client_refused(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
                                AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
        WHERE 
            TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 
            OR Enrollment.RelationshipToHoH = 1
        AND Enrollment.EntryDate <= %s
        AND (
            (Exit.ExitDate >= DATE_ADD(Enrollment.EntryDate, INTERVAL 1 YEAR) AND Exit.ExitDate > %s)
            OR (Exit.ExitDate IS NULL AND DATEDIFF(CURDATE(), Enrollment.EntryDate) >= 365)
        )
        AND IncomeBenefits.DataCollectionStage = 5
        AND (
            IncomeBenefits.InformationDate BETWEEN DATE_SUB(Enrollment.EntryDate, INTERVAL 30 DAY) 
            AND DATE_ADD(Enrollment.EntryDate, INTERVAL 30 DAY)
        )
        AND IncomeBenefits.IncomeFromAnySource IN (8, 9)
        '''
 
        
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH = 1)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND Enrollment.EntryDate <= %s 
        AND (
            (Exit.ExitDate >= DATE_ADD(Enrollment.EntryDate, INTERVAL 1 YEAR) AND Exit.ExitDate > %s)
            OR (Exit.ExitDate IS NULL AND DATEDIFF(CURDATE(), Enrollment.EntryDate) >= 365)
        )
                AND (IncomeBenefits.DataCollectionStage = 5)
        AND (
            IncomeBenefits.InformationDate BETWEEN DATE_SUB(Enrollment.EntryDate, INTERVAL 30 DAY) 
            AND DATE_ADD(Enrollment.EntryDate, INTERVAL 30 DAY)
        )
                AND IncomeBenefits.IncomeFromAnySource IN (8, 9)  '''            
                
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH = 1)
                AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
                AND Enrollment.DateOfEngagement <= %s
                AND Enrollment.EntryDate <= %s 
        AND (
            (Exit.ExitDate >= DATE_ADD(Enrollment.EntryDate, INTERVAL 1 YEAR) AND Exit.ExitDate > %s)
            OR (Exit.ExitDate IS NULL AND DATEDIFF(CURDATE(), Enrollment.EntryDate) >= 365)
        )
                AND (IncomeBenefits.DataCollectionStage = 5)
        AND (
            IncomeBenefits.InformationDate BETWEEN DATE_SUB(Enrollment.EntryDate, INTERVAL 30 DAY) 
            AND DATE_ADD(Enrollment.EntryDate, INTERVAL 30 DAY)
        )
                AND IncomeBenefits.IncomeFromAnySource IN (8, 9)  '''  

        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE Enrollment.EntryDate <= %s 
        AND (
            (Exit.ExitDate >= DATE_ADD(Enrollment.EntryDate, INTERVAL 1 YEAR) AND Exit.ExitDate > %s)
            OR (Exit.ExitDate IS NULL AND DATEDIFF(CURDATE(), Enrollment.EntryDate) >= 365)
        )
        AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18
        """
    ## EXIT DATE - ENTRY DATE IS GREATER THAN A YEAR AND IF NULL THEN THEY HAVE BEEN ENROLLED FOR ATELAST A YEAR?
        filter_params = [end_date, end_date]
        outreach_params = [end_date, end_date, end_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list
    #stayers
    def annual_income_missing(self,start_date, end_date, output=None, program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        sql_numerator_combined = '''
            SELECT 
                COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
                COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
            FROM Enrollment
            LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
            INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
            WHERE 
                (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH = 1)
                AND Enrollment.EntryDate <= %s 
                AND (
                    (Exit.ExitDate >= DATE_ADD(Enrollment.EntryDate, INTERVAL 1 YEAR) AND Exit.ExitDate > %s)
                    OR (Exit.ExitDate IS NULL AND DATEDIFF(CURDATE(), Enrollment.EntryDate) >= 365)
                )
                AND IncomeBenefits.DataCollectionStage = 5
                AND IncomeBenefits.InformationDate BETWEEN DATE_SUB(Enrollment.EntryDate, INTERVAL 30 DAY) 
                    AND DATE_ADD(Enrollment.EntryDate, INTERVAL 30 DAY)
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 1 AND 
                        (IncomeBenefits.Earned IS NULL OR IncomeBenefits.Unemployment IS NULL OR IncomeBenefits.SSI IS NULL 
                        OR IncomeBenefits.SSDI IS NULL OR IncomeBenefits.VADisabilityService IS NULL 
                        OR IncomeBenefits.VADisabilityNonService IS NULL OR IncomeBenefits.PrivateDisability IS NULL 
                        OR IncomeBenefits.WorkersComp IS NULL OR IncomeBenefits.TANF IS NULL OR IncomeBenefits.GA IS NULL 
                        OR IncomeBenefits.SocSecRetirement IS NULL OR IncomeBenefits.Pension IS NULL OR IncomeBenefits.ChildSupport IS NULL 
                        OR IncomeBenefits.Alimony IS NULL OR IncomeBenefits.OtherIncomeSource IS NULL))
                    OR IncomeBenefits.IncomeFromAnySource = 99 
                    OR IncomeBenefits.IncomeFromAnySource IS NULL
                )
        '''

        
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
            FROM AdditionalInformation
            LEFT JOIN Enrollment ON AdditionalInformation.EnrollmentID = Enrollment.EnrollmentID
            LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
            INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
            WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
            AND AdditionalInformation.StaffActiveStatus = 'Yes'
            AND (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH = 1)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
            AND Enrollment.EntryDate <= %s 
            AND (
                (Exit.ExitDate >= DATE_ADD(Enrollment.EntryDate, INTERVAL 1 YEAR) AND Exit.ExitDate > %s)
                OR (Exit.ExitDate IS NULL AND DATEDIFF(CURDATE(), Enrollment.EntryDate) >= 365)
            )
            AND (
                (
                    IncomeBenefits.DataCollectionStage = 5
                    AND IncomeBenefits.InformationDate BETWEEN DATE_SUB(Enrollment.EntryDate, INTERVAL 30 DAY) 
                    AND DATE_ADD(Enrollment.EntryDate, INTERVAL 30 DAY)
                    AND (
                        (IncomeBenefits.IncomeFromAnySource = 1 
                            AND (
                                IncomeBenefits.Earned IS NULL OR IncomeBenefits.Unemployment IS NULL OR IncomeBenefits.SSI IS NULL 
                                OR IncomeBenefits.SSDI IS NULL OR IncomeBenefits.VADisabilityService IS NULL
                                OR IncomeBenefits.VADisabilityNonService IS NULL OR IncomeBenefits.PrivateDisability IS NULL 
                                OR IncomeBenefits.WorkersComp IS NULL OR IncomeBenefits.TANF IS NULL
                                OR IncomeBenefits.GA IS NULL OR IncomeBenefits.SocSecRetirement IS NULL 
                                OR IncomeBenefits.Pension IS NULL OR IncomeBenefits.ChildSupport IS NULL 
                                OR IncomeBenefits.Alimony IS NULL OR IncomeBenefits.OtherIncomeSource IS NULL
                            )
                        )
                        OR IncomeBenefits.IncomeFromAnySource = 99
                        OR IncomeBenefits.IncomeFromAnySource IS NULL
                    )
                )
                OR (
                    IncomeBenefits.DataCollectionStage = 5
                    AND IncomeBenefits.InformationDate BETWEEN DATE_SUB(Enrollment.EntryDate, INTERVAL 30 DAY) 
                    AND DATE_ADD(Enrollment.EntryDate, INTERVAL 30 DAY)
                )
            )       
        '''

        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
            FROM AdditionalInformation
            LEFT JOIN Enrollment ON AdditionalInformation.EnrollmentID = Enrollment.EnrollmentID
            LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
            INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
            WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
            AND AdditionalInformation.StaffActiveStatus = 'Yes'
            AND (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH = 1)
            AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'
            AND Enrollment.DateOfEngagement <= %s
            AND Enrollment.EntryDate <= %s 
            AND (
                (Exit.ExitDate >= DATE_ADD(Enrollment.EntryDate, INTERVAL 1 YEAR) AND Exit.ExitDate > %s)
                OR (Exit.ExitDate IS NULL AND DATEDIFF(CURDATE(), Enrollment.EntryDate) >= 365)
            )
            AND (
                (
                    IncomeBenefits.DataCollectionStage = 5
                    AND IncomeBenefits.InformationDate BETWEEN DATE_SUB(Enrollment.EntryDate, INTERVAL 30 DAY) 
                    AND DATE_ADD(Enrollment.EntryDate, INTERVAL 30 DAY)
                    AND (
                        (IncomeBenefits.IncomeFromAnySource = 1 
                            AND (
                                IncomeBenefits.Earned IS NULL OR IncomeBenefits.Unemployment IS NULL OR IncomeBenefits.SSI IS NULL 
                                OR IncomeBenefits.SSDI IS NULL OR IncomeBenefits.VADisabilityService IS NULL
                                OR IncomeBenefits.VADisabilityNonService IS NULL OR IncomeBenefits.PrivateDisability IS NULL 
                                OR IncomeBenefits.WorkersComp IS NULL OR IncomeBenefits.TANF IS NULL
                                OR IncomeBenefits.GA IS NULL OR IncomeBenefits.SocSecRetirement IS NULL 
                                OR IncomeBenefits.Pension IS NULL OR IncomeBenefits.ChildSupport IS NULL 
                                OR IncomeBenefits.Alimony IS NULL OR IncomeBenefits.OtherIncomeSource IS NULL
                            )
                        )
                        OR IncomeBenefits.IncomeFromAnySource = 99
                        OR IncomeBenefits.IncomeFromAnySource IS NULL
                    )
                )
                OR (
                    IncomeBenefits.DataCollectionStage = 5
                    AND IncomeBenefits.InformationDate BETWEEN DATE_SUB(Enrollment.EntryDate, INTERVAL 30 DAY) 
                    AND DATE_ADD(Enrollment.EntryDate, INTERVAL 30 DAY)
                )
            )       
        '''
 
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE Enrollment.EntryDate <= %s 
        AND (
            (Exit.ExitDate >= DATE_ADD(Enrollment.EntryDate, INTERVAL 1 YEAR) AND Exit.ExitDate > %s)
            OR (Exit.ExitDate IS NULL AND DATEDIFF(CURDATE(), Enrollment.EntryDate) >= 365)
        )
        AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18
        """

        filter_params = [end_date, end_date]
        outreach_params = [end_date, end_date, end_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list

    #stayers
    def annual_income_data_accuracy(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
            conn=mysql.connector.connect(**server)
            cursor=conn.cursor()

            cursor.execute(f"USE `{db}`")
            
            sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
            WHERE (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH = 1)
            AND (Enrollment.EntryDate <= %s) 
        AND (
            (Exit.ExitDate >= DATE_ADD(Enrollment.EntryDate, INTERVAL 1 YEAR) AND Exit.ExitDate > %s)
            OR (Exit.ExitDate IS NULL AND DATEDIFF(CURDATE(), Enrollment.EntryDate) >= 365)
        )
                AND (IncomeBenefits.DataCollectionStage = 5)
        AND (
            IncomeBenefits.InformationDate BETWEEN DATE_SUB(Enrollment.EntryDate, INTERVAL 30 DAY) 
            AND DATE_ADD(Enrollment.EntryDate, INTERVAL 30 DAY)
        )
                AND ((IncomeBenefits.IncomeFromAnySource=1)
                            AND IncomeBenefits.Earned = 0
                            AND IncomeBenefits.Unemployment = 0
                            AND IncomeBenefits.SSI = 0
                            AND IncomeBenefits.SSDI = 0
                            AND IncomeBenefits.VADisabilityService = 0
                            AND IncomeBenefits.VADisabilityNonService = 0
                            AND IncomeBenefits.PrivateDisability = 0
                            AND IncomeBenefits.WorkersComp = 0
                            AND IncomeBenefits.TANF = 0
                            AND IncomeBenefits.GA = 0
                            AND IncomeBenefits.SocSecRetirement = 0
                            AND IncomeBenefits.Pension = 0
                            AND IncomeBenefits.ChildSupport = 0
                            AND IncomeBenefits.Alimony = 0
                            AND IncomeBenefits.OtherIncomeSource = 0)
                
        '''
            sql_non_outreach_list = '''
        SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
            FROM AdditionalInformation
            LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
            LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
            INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
            WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
            AND AdditionalInformation.StaffActiveStatus ='Yes'
            AND (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH = 1)
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= %s 
        AND (
            (Exit.ExitDate >= DATE_ADD(Enrollment.EntryDate, INTERVAL 1 YEAR) AND Exit.ExitDate > %s)
            OR (Exit.ExitDate IS NULL AND DATEDIFF(CURDATE(), Enrollment.EntryDate) >= 365)
        )
            AND (IncomeBenefits.DataCollectionStage = 5)
        AND (
            IncomeBenefits.InformationDate BETWEEN DATE_SUB(Enrollment.EntryDate, INTERVAL 30 DAY) 
            AND DATE_ADD(Enrollment.EntryDate, INTERVAL 30 DAY)
        )
            AND ((IncomeBenefits.IncomeFromAnySource=1)
                        AND IncomeBenefits.Earned = 0
                        AND IncomeBenefits.Unemployment = 0
                        AND IncomeBenefits.SSI = 0
                        AND IncomeBenefits.SSDI = 0
                        AND IncomeBenefits.VADisabilityService = 0
                        AND IncomeBenefits.VADisabilityNonService = 0
                        AND IncomeBenefits.PrivateDisability = 0
                        AND IncomeBenefits.WorkersComp = 0
                        AND IncomeBenefits.TANF = 0
                        AND IncomeBenefits.GA = 0
                        AND IncomeBenefits.SocSecRetirement = 0
                        AND IncomeBenefits.Pension = 0
                        AND IncomeBenefits.ChildSupport = 0
                        AND IncomeBenefits.Alimony = 0
                        AND IncomeBenefits.OtherIncomeSource = 0)'''
            sql_outreach_list = '''
        SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
            FROM AdditionalInformation
            LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
            LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
            INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
            WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
            AND AdditionalInformation.StaffActiveStatus ='Yes'
            AND (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH = 1)
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
        AND Enrollment.DateOfEngagement <= %s
        AND Enrollment.EntryDate <= %s 
        AND (
            (Exit.ExitDate >= DATE_ADD(Enrollment.EntryDate, INTERVAL 1 YEAR) AND Exit.ExitDate > %s)
            OR (Exit.ExitDate IS NULL AND DATEDIFF(CURDATE(), Enrollment.EntryDate) >= 365)
        )
            AND (IncomeBenefits.DataCollectionStage = 5)
        AND (
            IncomeBenefits.InformationDate BETWEEN DATE_SUB(Enrollment.EntryDate, INTERVAL 30 DAY) 
            AND DATE_ADD(Enrollment.EntryDate, INTERVAL 30 DAY)
        )
            AND ((IncomeBenefits.IncomeFromAnySource=1)
                        AND IncomeBenefits.Earned = 0
                        AND IncomeBenefits.Unemployment = 0
                        AND IncomeBenefits.SSI = 0
                        AND IncomeBenefits.SSDI = 0
                        AND IncomeBenefits.VADisabilityService = 0
                        AND IncomeBenefits.VADisabilityNonService = 0
                        AND IncomeBenefits.PrivateDisability = 0
                        AND IncomeBenefits.WorkersComp = 0
                        AND IncomeBenefits.TANF = 0
                        AND IncomeBenefits.GA = 0
                        AND IncomeBenefits.SocSecRetirement = 0
                        AND IncomeBenefits.Pension = 0
                        AND IncomeBenefits.ChildSupport = 0
                        AND IncomeBenefits.Alimony = 0
                        AND IncomeBenefits.OtherIncomeSource = 0)'''                    
            sql_denominator_combined = """
            SELECT 
                COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
                COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
            FROM Enrollment
            LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
            WHERE Enrollment.EntryDate <= %s 
        AND (
            (Exit.ExitDate >= DATE_ADD(Enrollment.EntryDate, INTERVAL 1 YEAR) AND Exit.ExitDate > %s)
            OR (Exit.ExitDate IS NULL AND DATEDIFF(CURDATE(), Enrollment.EntryDate) >= 365)
        )
            AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18
            """

            filter_params = [end_date, end_date]
            outreach_params = [end_date, end_date, end_date]

            if program_id is not None:
                placeholders = ','.join(['%s' for _ in program_id])
                sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
                filter_params.extend(program_id)
                outreach_params.extend(program_id)

            if department is not None:
                placeholders = ','.join(['%s' for _ in department])
                sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
                filter_params.extend(department)
                outreach_params.extend(department)

            if region is not None:
                placeholders = ','.join(['%s' for _ in region])
                sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
                filter_params.extend(region)
                outreach_params.extend(region)

            if program_type is not None:
                placeholders = ','.join(['%s' for _ in program_type])
                sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
                filter_params.extend(program_type)
                outreach_params.extend(program_type)
                
            cursor.execute(sql_numerator_combined, outreach_params)
            scores=cursor.fetchone()
            non_outreach_score=scores[0]
            outreach_scores=scores[1]
            
            cursor.execute(sql_denominator_combined, outreach_params)
            result=cursor.fetchone()
            total_non_outreach = result[0]
            total_outreach=result[1]
            
            cursor.execute(sql_non_outreach_list, filter_params)
            non_outreach_pt_list = cursor.fetchall()
            
            cursor.execute(sql_outreach_list, outreach_params)
            outreach_pt_list = cursor.fetchall()
            
            conn.close()

            total_score=non_outreach_score+outreach_scores
            total_enrollments = total_outreach + total_non_outreach
            full_list=non_outreach_pt_list+outreach_pt_list
            if output==None:
                if isinstance(total_score, numbers.Number):                       
                    if total_enrollments > 0:
                        if 1-(total_score/total_enrollments) <= 0:
                            return 0
                        else:
                            return 1-(total_score/total_enrollments)
                    else:
                        return 0
                else:
                    return None
            elif output =="list":
                return full_list

    def exiting_income_total_accuracy(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None,server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
                WHERE (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH=1)
                AND (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
                AND (Exit.ExitDate =IncomeBenefits.InformationDate)
                AND (
            (
                Exit.ExitDate = IncomeBenefits.InformationDate
                AND (IncomeBenefits.DataCollectionStage = 3)
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 8 OR IncomeBenefits.IncomeFromAnySource = 9)
                )
            )
            OR (
                Exit.ExitDate = IncomeBenefits.InformationDate
                AND (IncomeBenefits.DataCollectionStage = 3)
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 1 AND (
                            IncomeBenefits.Earned IS NULL OR IncomeBenefits.Unemployment IS NULL OR IncomeBenefits.SSI IS NULL 
                            OR IncomeBenefits.SSDI IS NULL OR IncomeBenefits.VADisabilityService IS NULL
                            OR IncomeBenefits.VADisabilityNonService IS NULL OR IncomeBenefits.PrivateDisability IS NULL 
                            OR IncomeBenefits.WorkersComp IS NULL OR IncomeBenefits.TANF IS NULL
                            OR IncomeBenefits.GA IS NULL OR IncomeBenefits.SocSecRetirement IS NULL 
                            OR IncomeBenefits.Pension IS NULL OR IncomeBenefits.ChildSupport IS NULL 
                            OR IncomeBenefits.Alimony IS NULL OR IncomeBenefits.OtherIncomeSource IS NULL
                        )
                    )
                    OR (IncomeBenefits.IncomeFromAnySource = 99 or IncomeBenefits.IncomeFromAnySource is NULL)
                )
            )
            OR (
                Exit.ExitDate != IncomeBenefits.InformationDate
                AND (IncomeBenefits.DataCollectionStage = 3)
            )
            OR (
                Exit.ExitDate = IncomeBenefits.InformationDate
                AND (IncomeBenefits.DataCollectionStage = 3)
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 1)
                    AND IncomeBenefits.Earned = 0
                    AND IncomeBenefits.Unemployment = 0
                    AND IncomeBenefits.SSI = 0
                    AND IncomeBenefits.SSDI = 0
                    AND IncomeBenefits.VADisabilityService = 0
                    AND IncomeBenefits.VADisabilityNonService = 0
                    AND IncomeBenefits.PrivateDisability = 0
                    AND IncomeBenefits.WorkersComp = 0
                    AND IncomeBenefits.TANF = 0
                    AND IncomeBenefits.GA = 0
                    AND IncomeBenefits.SocSecRetirement = 0
                    AND IncomeBenefits.Pension = 0
                    AND IncomeBenefits.ChildSupport = 0
                    AND IncomeBenefits.Alimony = 0
                    AND IncomeBenefits.OtherIncomeSource = 0
                )
            )
    )
        '''


        sql_non_outreach_list = '''
        SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier,AdditionalInformation.CaseManager
                From AdditionalInformation
                LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH=1)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'                
                AND (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
                AND (Exit.ExitDate =IncomeBenefits.InformationDate)
                AND (
            (
                Exit.ExitDate = IncomeBenefits.InformationDate
                AND (IncomeBenefits.DataCollectionStage = 3)
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 8 OR IncomeBenefits.IncomeFromAnySource = 9)
                )
            )
            OR (
                Exit.ExitDate = IncomeBenefits.InformationDate
                AND (IncomeBenefits.DataCollectionStage = 3)
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 1 AND (
                            IncomeBenefits.Earned IS NULL OR IncomeBenefits.Unemployment IS NULL OR IncomeBenefits.SSI IS NULL 
                            OR IncomeBenefits.SSDI IS NULL OR IncomeBenefits.VADisabilityService IS NULL
                            OR IncomeBenefits.VADisabilityNonService IS NULL OR IncomeBenefits.PrivateDisability IS NULL 
                            OR IncomeBenefits.WorkersComp IS NULL OR IncomeBenefits.TANF IS NULL
                            OR IncomeBenefits.GA IS NULL OR IncomeBenefits.SocSecRetirement IS NULL 
                            OR IncomeBenefits.Pension IS NULL OR IncomeBenefits.ChildSupport IS NULL 
                            OR IncomeBenefits.Alimony IS NULL OR IncomeBenefits.OtherIncomeSource IS NULL
                        )
                    )
                    OR (IncomeBenefits.IncomeFromAnySource = 99 or IncomeBenefits.IncomeFromAnySource is NULL)
                )
            )
            OR (
                Exit.ExitDate != IncomeBenefits.InformationDate
                AND (IncomeBenefits.DataCollectionStage = 3)
            )
            OR (
                Exit.ExitDate = IncomeBenefits.InformationDate
                AND (IncomeBenefits.DataCollectionStage = 3)
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 1)
                    AND IncomeBenefits.Earned = 0
                    AND IncomeBenefits.Unemployment = 0
                    AND IncomeBenefits.SSI = 0
                    AND IncomeBenefits.SSDI = 0
                    AND IncomeBenefits.VADisabilityService = 0
                    AND IncomeBenefits.VADisabilityNonService = 0
                    AND IncomeBenefits.PrivateDisability = 0
                    AND IncomeBenefits.WorkersComp = 0
                    AND IncomeBenefits.TANF = 0
                    AND IncomeBenefits.GA = 0
                    AND IncomeBenefits.SocSecRetirement = 0
                    AND IncomeBenefits.Pension = 0
                    AND IncomeBenefits.ChildSupport = 0
                    AND IncomeBenefits.Alimony = 0
                    AND IncomeBenefits.OtherIncomeSource = 0
                )
            )
    )
        '''
        sql_outreach_list = '''
        SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier,AdditionalInformation.CaseManager
                From AdditionalInformation
                LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH=1)
                AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
                AND Enrollment.DateOfEngagement <= %s               
                AND (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
                AND (Exit.ExitDate =IncomeBenefits.InformationDate)
                AND (
            (
                Exit.ExitDate = IncomeBenefits.InformationDate
                AND (IncomeBenefits.DataCollectionStage = 3)
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 8 OR IncomeBenefits.IncomeFromAnySource = 9)
                )
            )
            OR (
                Exit.ExitDate = IncomeBenefits.InformationDate
                AND (IncomeBenefits.DataCollectionStage = 3)
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 1 AND (
                            IncomeBenefits.Earned IS NULL OR IncomeBenefits.Unemployment IS NULL OR IncomeBenefits.SSI IS NULL 
                            OR IncomeBenefits.SSDI IS NULL OR IncomeBenefits.VADisabilityService IS NULL
                            OR IncomeBenefits.VADisabilityNonService IS NULL OR IncomeBenefits.PrivateDisability IS NULL 
                            OR IncomeBenefits.WorkersComp IS NULL OR IncomeBenefits.TANF IS NULL
                            OR IncomeBenefits.GA IS NULL OR IncomeBenefits.SocSecRetirement IS NULL 
                            OR IncomeBenefits.Pension IS NULL OR IncomeBenefits.ChildSupport IS NULL 
                            OR IncomeBenefits.Alimony IS NULL OR IncomeBenefits.OtherIncomeSource IS NULL
                        )
                    )
                    OR (IncomeBenefits.IncomeFromAnySource = 99 or IncomeBenefits.IncomeFromAnySource is NULL)
                )
            )
            OR (
                Exit.ExitDate != IncomeBenefits.InformationDate
                AND (IncomeBenefits.DataCollectionStage = 3)
            )
            OR (
                Exit.ExitDate = IncomeBenefits.InformationDate
                AND (IncomeBenefits.DataCollectionStage = 3)
                AND (
                    (IncomeBenefits.IncomeFromAnySource = 1)
                    AND IncomeBenefits.Earned = 0
                    AND IncomeBenefits.Unemployment = 0
                    AND IncomeBenefits.SSI = 0
                    AND IncomeBenefits.SSDI = 0
                    AND IncomeBenefits.VADisabilityService = 0
                    AND IncomeBenefits.VADisabilityNonService = 0
                    AND IncomeBenefits.PrivateDisability = 0
                    AND IncomeBenefits.WorkersComp = 0
                    AND IncomeBenefits.TANF = 0
                    AND IncomeBenefits.GA = 0
                    AND IncomeBenefits.SocSecRetirement = 0
                    AND IncomeBenefits.Pension = 0
                    AND IncomeBenefits.ChildSupport = 0
                    AND IncomeBenefits.Alimony = 0
                    AND IncomeBenefits.OtherIncomeSource = 0
                )
            )
    )
        '''                
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
        AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18
        """



        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list
        
    def exiting_income_client_refused(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
                WHERE (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH=1)
                AND (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
                AND (Exit.ExitDate =IncomeBenefits.InformationDate)
                AND (IncomeBenefits.DataCollectionStage=3)
                AND (IncomeBenefits.IncomeFromAnySource IN (8,9))             
            '''
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH=1)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
                AND (Exit.ExitDate =IncomeBenefits.InformationDate)
                AND (IncomeBenefits.DataCollectionStage=3)
                AND (IncomeBenefits.IncomeFromAnySource IN (8,9)) '''             
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH=1)
                AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
                AND Enrollment.DateOfEngagement <= %s
                AND (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
                AND (Exit.ExitDate =IncomeBenefits.InformationDate)
                AND (IncomeBenefits.DataCollectionStage=3)
                AND (IncomeBenefits.IncomeFromAnySource IN (8,9)) '''            
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
        AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list
        
    def exiting_income_client_missing(self,start_date, end_date, output=None, program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
            WHERE  (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH = 1)
                AND (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
                AND (Exit.ExitDate =IncomeBenefits.InformationDate)
                AND (
                (
                    Exit.ExitDate = IncomeBenefits.InformationDate
                    AND (IncomeBenefits.DataCollectionStage = 3)
                    AND (
                        (IncomeBenefits.IncomeFromAnySource = 1 AND (
                                IncomeBenefits.Earned IS NULL OR IncomeBenefits.Unemployment IS NULL OR IncomeBenefits.SSI IS NULL 
                                OR IncomeBenefits.SSDI IS NULL OR IncomeBenefits.VADisabilityService IS NULL
                                OR IncomeBenefits.VADisabilityNonService IS NULL OR IncomeBenefits.PrivateDisability IS NULL 
                                OR IncomeBenefits.WorkersComp IS NULL OR IncomeBenefits.TANF IS NULL
                                OR IncomeBenefits.GA IS NULL OR IncomeBenefits.SocSecRetirement IS NULL 
                                OR IncomeBenefits.Pension IS NULL OR IncomeBenefits.ChildSupport IS NULL 
                                OR IncomeBenefits.Alimony IS NULL OR IncomeBenefits.OtherIncomeSource IS NULL
                            )
                        )
                        OR (IncomeBenefits.IncomeFromAnySource=99 or IncomeBenefits.IncomeFromAnySource is NULL)
                    )
                )
                OR (
                    Exit.ExitDate != IncomeBenefits.InformationDate
                    AND (IncomeBenefits.DataCollectionStage = 3)
                )
            )
                
        '''

        sql_non_outreach_list = '''
        SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier,AdditionalInformation.CaseManager
                From AdditionalInformation
                LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH=1)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
                AND (Exit.ExitDate =IncomeBenefits.InformationDate)
                AND (
                (
                    Exit.ExitDate = IncomeBenefits.InformationDate
                    AND (IncomeBenefits.DataCollectionStage = 3)
                    AND (
                        (IncomeBenefits.IncomeFromAnySource = 1 AND (
                                IncomeBenefits.Earned IS NULL OR IncomeBenefits.Unemployment IS NULL OR IncomeBenefits.SSI IS NULL 
                                OR IncomeBenefits.SSDI IS NULL OR IncomeBenefits.VADisabilityService IS NULL
                                OR IncomeBenefits.VADisabilityNonService IS NULL OR IncomeBenefits.PrivateDisability IS NULL 
                                OR IncomeBenefits.WorkersComp IS NULL OR IncomeBenefits.TANF IS NULL
                                OR IncomeBenefits.GA IS NULL OR IncomeBenefits.SocSecRetirement IS NULL 
                                OR IncomeBenefits.Pension IS NULL OR IncomeBenefits.ChildSupport IS NULL 
                                OR IncomeBenefits.Alimony IS NULL OR IncomeBenefits.OtherIncomeSource IS NULL
                            )
                        )
                        OR (IncomeBenefits.IncomeFromAnySource=99 or IncomeBenefits.IncomeFromAnySource is NULL)
                    )
                )
                OR (
                    Exit.ExitDate != IncomeBenefits.InformationDate
                    AND (IncomeBenefits.DataCollectionStage = 3)
                )
            )       
        '''
        sql_outreach_list = '''
        SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier,AdditionalInformation.CaseManager
                From AdditionalInformation
                LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH=1)
            AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
            AND Enrollment.DateOfEngagement <= %s
                AND (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
                AND (Exit.ExitDate =IncomeBenefits.InformationDate)
                AND (
                (
                    Exit.ExitDate = IncomeBenefits.InformationDate
                    AND (IncomeBenefits.DataCollectionStage = 3)
                    AND (
                        (IncomeBenefits.IncomeFromAnySource = 1 AND (
                                IncomeBenefits.Earned IS NULL OR IncomeBenefits.Unemployment IS NULL OR IncomeBenefits.SSI IS NULL 
                                OR IncomeBenefits.SSDI IS NULL OR IncomeBenefits.VADisabilityService IS NULL
                                OR IncomeBenefits.VADisabilityNonService IS NULL OR IncomeBenefits.PrivateDisability IS NULL 
                                OR IncomeBenefits.WorkersComp IS NULL OR IncomeBenefits.TANF IS NULL
                                OR IncomeBenefits.GA IS NULL OR IncomeBenefits.SocSecRetirement IS NULL 
                                OR IncomeBenefits.Pension IS NULL OR IncomeBenefits.ChildSupport IS NULL 
                                OR IncomeBenefits.Alimony IS NULL OR IncomeBenefits.OtherIncomeSource IS NULL
                            )
                        )
                        OR (IncomeBenefits.IncomeFromAnySource=99 or IncomeBenefits.IncomeFromAnySource is NULL)
                    )
                )
                OR (
                    Exit.ExitDate != IncomeBenefits.InformationDate
                    AND (IncomeBenefits.DataCollectionStage = 3)
                )
            )       
        '''                
        
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
        AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list

    def exiting_income_data_accuracy(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
            
        sql_numerator_combined = '''
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
                WHERE (TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH=1)
                AND (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
                AND (Exit.ExitDate =IncomeBenefits.InformationDate)
                AND (IncomeBenefits.DataCollectionStage=3)
				AND ((IncomeBenefits.IncomeFromAnySource=1)
                    AND IncomeBenefits.Earned = 0
                    AND IncomeBenefits.Unemployment = 0
                    AND IncomeBenefits.SSI = 0
                    AND IncomeBenefits.SSDI = 0
                    AND IncomeBenefits.VADisabilityService = 0
                    AND IncomeBenefits.VADisabilityNonService = 0
                    AND IncomeBenefits.PrivateDisability = 0
                    AND IncomeBenefits.WorkersComp = 0
                    AND IncomeBenefits.TANF = 0
                    AND IncomeBenefits.GA = 0
                    AND IncomeBenefits.SocSecRetirement = 0
                    AND IncomeBenefits.Pension = 0
                    AND IncomeBenefits.ChildSupport = 0
                    AND IncomeBenefits.Alimony = 0
                    AND IncomeBenefits.OtherIncomeSource = 0)
            '''
        sql_non_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND(TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH=1)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'                
                AND (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
                AND (Exit.ExitDate =IncomeBenefits.InformationDate)
                AND (IncomeBenefits.DataCollectionStage=3)
				AND ((IncomeBenefits.IncomeFromAnySource=1)
                    AND IncomeBenefits.Earned = 0
                    AND IncomeBenefits.Unemployment = 0
                    AND IncomeBenefits.SSI = 0
                    AND IncomeBenefits.SSDI = 0
                    AND IncomeBenefits.VADisabilityService = 0
                    AND IncomeBenefits.VADisabilityNonService = 0
                    AND IncomeBenefits.PrivateDisability = 0
                    AND IncomeBenefits.WorkersComp = 0
                    AND IncomeBenefits.TANF = 0
                    AND IncomeBenefits.GA = 0
                    AND IncomeBenefits.SocSecRetirement = 0
                    AND IncomeBenefits.Pension = 0
                    AND IncomeBenefits.ChildSupport = 0
                    AND IncomeBenefits.Alimony = 0
                    AND IncomeBenefits.OtherIncomeSource = 0)'''       
        sql_outreach_list = '''
            SELECT DISTINCT AdditionalInformation.AdditionalInformationentifier, AdditionalInformation.CaseManager
                FROM AdditionalInformation
				LEFT JOIN Enrollment on AdditionalInformation.EnrollmentID=Enrollment.EnrollmentID
                LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.EnrollmentID = IncomeBenefits.EnrollmentID
                WHERE AdditionalInformation.StaffHomeAgency LIKE '%PATH%'
				AND AdditionalInformation.StaffActiveStatus ='Yes'
                AND(TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18 OR Enrollment.RelationshipToHoH=1)
                AND PATHProgramMasterList.PATHProgramType = 'Outreach Services' 
                AND Enrollment.DateOfEngagement <= %s              
                AND (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
                AND (Exit.ExitDate =IncomeBenefits.InformationDate)
                AND (IncomeBenefits.DataCollectionStage=3)
				AND ((IncomeBenefits.IncomeFromAnySource=1)
                    AND IncomeBenefits.Earned = 0
                    AND IncomeBenefits.Unemployment = 0
                    AND IncomeBenefits.SSI = 0
                    AND IncomeBenefits.SSDI = 0
                    AND IncomeBenefits.VADisabilityService = 0
                    AND IncomeBenefits.VADisabilityNonService = 0
                    AND IncomeBenefits.PrivateDisability = 0
                    AND IncomeBenefits.WorkersComp = 0
                    AND IncomeBenefits.TANF = 0
                    AND IncomeBenefits.GA = 0
                    AND IncomeBenefits.SocSecRetirement = 0
                    AND IncomeBenefits.Pension = 0
                    AND IncomeBenefits.ChildSupport = 0
                    AND IncomeBenefits.Alimony = 0
                    AND IncomeBenefits.OtherIncomeSource = 0)'''              
        sql_denominator_combined = """
        SELECT 
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType != 'Outreach Services' THEN Enrollment.PersonalID END) AS non_outreach_count,
            COUNT(DISTINCT CASE WHEN PATHProgramMasterList.PATHProgramType = 'Outreach Services' AND Enrollment.DateOfEngagement <= %s THEN Enrollment.PersonalID END) AS outreach_count
        FROM Enrollment
        LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
        AND TIMESTAMPDIFF(YEAR, Client.DOB, Enrollment.EntryDate) > 18
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            sql_numerator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            sql_numerator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            sql_numerator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            sql_numerator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_non_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_outreach_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_denominator_combined += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        cursor.execute(sql_numerator_combined, outreach_params)
        scores=cursor.fetchone()
        non_outreach_score=scores[0]
        outreach_scores=scores[1]
        
        cursor.execute(sql_denominator_combined, outreach_params)
        result=cursor.fetchone()
        total_non_outreach = result[0]
        total_outreach=result[1]
        
        cursor.execute(sql_non_outreach_list, filter_params)
        non_outreach_pt_list = cursor.fetchall()
        
        cursor.execute(sql_outreach_list, outreach_params)
        outreach_pt_list = cursor.fetchall()
        
        conn.close()

        total_score=non_outreach_score+outreach_scores
        total_enrollments = total_outreach + total_non_outreach
        full_list=non_outreach_pt_list+outreach_pt_list
        if output==None:
            if isinstance(total_score, numbers.Number):                       
                if total_enrollments > 0:
                    if 1-(total_score/total_enrollments) <= 0:
                        return 0
                    else:
                        return 1-(total_score/total_enrollments)
                else:
                    return 0
            else:
                return None
        elif output =="list":
            return full_list
class Timeliness():
    def record_creation_start_average(self,start_date, end_date, program_id=None, department=None, region=None, program_type=None,server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        days_to_create = '''
            SELECT 
                CASE
                    WHEN DATEDIFF(MAX(Enrollment.DateCreated), MAX(Enrollment.EntryDate)) < 0 THEN 0
                    ELSE DATEDIFF(MAX(Enrollment.DateCreated), MAX(Enrollment.EntryDate))
                END AS DaysBetween
            FROM Enrollment
            LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE Enrollment.EntryDate <= %s
            AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
            GROUP BY Enrollment.PersonalID
        '''


        filter_params = [end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            days_to_create += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            days_to_create += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            days_to_create += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            days_to_create += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)

        cursor.execute(days_to_create, filter_params)
        days_to_create_count = cursor.fetchall()
        
        days_to_create_list = [row[0] for row in days_to_create_count if row[0] is not None]
        
        conn.close()
        if len(days_to_create_list)==0:
            return 0
        else:
            return mean(days_to_create_list)

    #created looking at "leavers"
    def record_creation_exit_average(self,start_date, end_date, program_id=None, department=None, region=None, program_type=None,server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        days_to_create = '''
            SELECT 
                CASE
                    WHEN DATEDIFF(MAX(Enrollment.DateCreated), MAX(Enrollment.EntryDate)) < 0 THEN 0
                    ELSE DATEDIFF(MAX(Enrollment.DateCreated), MAX(Enrollment.EntryDate))
                END AS DaysBetween
            FROM Enrollment
            LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
            GROUP BY Enrollment.PersonalID
        '''

        filter_params = [end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            days_to_create += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            days_to_create += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            days_to_create += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            days_to_create += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)

        cursor.execute(days_to_create, filter_params)
        days_to_create_count = cursor.fetchall()
        
        days_to_create_list = [row[0] for row in days_to_create_count if row[0] is not None]
        
        conn.close()

        if len(days_to_create_list)==0:
            return 0
        else:
            return mean(days_to_create_list)

    def percent_start_records_created_within_x_days(self,start_date, end_date, days, output=None,program_id=None, department=None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        start_sql = '''
            SELECT 
                CASE
                    WHEN DATEDIFF(MAX(Enrollment.DateCreated), MAX(Enrollment.EntryDate)) < 0 THEN 0
                    ELSE DATEDIFF(MAX(Enrollment.DateCreated), MAX(Enrollment.EntryDate))
                END AS DaysBetween
            FROM Enrollment
            LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE Enrollment.EntryDate <= %s 
            AND (Exit.ExitDate >= %s OR Exit.ExitDate IS NULL)
            GROUP BY Enrollment.PersonalID
        '''

        filter_params = [end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            start_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)


        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            start_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            start_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)


        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            start_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)


        cursor.execute(start_sql, filter_params)
        start_count = cursor.fetchall()
        
        days_to_create_list = [row[0] for row in start_count if row[0] is not None]

        conn.close()

        total_start = len(days_to_create_list)
        count_created_within_x_days = sum(1 for numdays in days_to_create_list if numdays <= days)
        if output==None:
            if total_start > 0:
                return (count_created_within_x_days / total_start)
            else:
                return 0
        elif output =="count":
            return total_start-count_created_within_x_days

    def percent_exit_records_created_within_x_days(self,start_date, end_date, days, output=None,program_id=None, department=None, region=None, program_type=None, server=server_config,db=database_name):
        conn=mysql.connector.connect(**server)
        cursor=conn.cursor()

        cursor.execute(f"USE `{db}`")
        
        exit_sql = '''
            SELECT 
                CASE
                    WHEN DATEDIFF(MAX(Enrollment.DateCreated), MAX(Enrollment.EntryDate)) < 0 THEN 0
                    ELSE DATEDIFF(MAX(Enrollment.DateCreated), MAX(Enrollment.EntryDate))
                END AS DaysBetween
            FROM Enrollment
            LEFT JOIN `Exit` ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE (Exit.ExitDate <= %s AND Exit.ExitDate >= %s)
            GROUP BY Enrollment.PersonalID
        '''

        filter_params = [end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['%s' for _ in program_id])
            exit_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)


        if department is not None:
            placeholders = ','.join(['%s' for _ in department])
            exit_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)

        if region is not None:
            placeholders = ','.join(['%s' for _ in region])
            exit_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)


        if program_type is not None:
            placeholders = ','.join(['%s' for _ in program_type])
            exit_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)


        cursor.execute(exit_sql, filter_params)
        exit_count = cursor.fetchall()
        
        days_to_create_list = [row[0] for row in exit_count if row[0] is not None]

        conn.close()

        total_exit = len(days_to_create_list)
        count_created_within_x_days = sum(1 for numdays in days_to_create_list if numdays <= days)
        if output==None:
            if total_exit > 0:
                return (count_created_within_x_days / total_exit)
            else:
                return 0
        elif output=="count":
            return total_exit-count_created_within_x_days
        


    



import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import PercentFormatter
def line_chart_specific_days(function, filename):
    department_colors = {
    'San Diego': '#FFB7B2',
    'Santa Barbara': '#FFDAC1',
    'Orange County': '#E2F0CB',
    'Santa Clara': '#B5EAD7',
    'Families': '#C7CEEA',
    'Metro LA': '#E6C8FE',
    'Permanent Supportive Services': '#8097BA',
    'South County': '#F3D88E',
    'Veterans': '#A7D1AD',
    'West LA': '#A7D1D1',
    }

    days_of_interest = [1, 4, 7, 11, 14]

    # Create an instance of the class containing the method
    timeliness_instance = Timeliness()

    # Collect results for the specific days
    results_by_department = {d: [] for d in department_colors}
    agency_results = []

    for days in days_of_interest:
        parameters = {"start_date": "2024-01-01", "end_date": "2024-01-31", "days": days}
        agency_number = function(timeliness_instance, **parameters)
        agency_results.append(agency_number)

        for d, color in department_colors.items():
            parameters["department"] = [d]
            number = function(timeliness_instance, **parameters)
            results_by_department[d].append(number)

    # Create the line plot
    width = 3.625
    height = 2.167
    image_dpi = 600
    fig, ax = plt.subplots(figsize=(width, height), dpi=image_dpi)

    for d, values in results_by_department.items():
        ax.plot(days_of_interest, values, linestyle='-', label=f'{d}', linewidth=0.5, markersize=2, color=department_colors[d])

    ax.plot(days_of_interest, agency_results, linestyle='-', label='Agency', linewidth=.75, marker='o',markersize=1, color='black')
    ax.axhline(0, color='white', linestyle='--')

    # Set x-axis tick labels with font size
    ax.set_xticks(days_of_interest)
    ax.set_xticklabels([f'Day {days}' for days in days_of_interest], fontsize=4, ha='center')

    ax.yaxis.set_major_formatter(PercentFormatter(100))
    ax.set_yticklabels([f'{tick * 100:.1f}%' for tick in ax.get_yticks()], fontsize=4)

    # Set y-axis limit to start from zero

    ax.legend(loc='upper left', bbox_to_anchor=(-0.025,-0.1), fontsize=4, ncol=4)

    # Save the plot as an image
    plt.savefig(filename, bbox_inches='tight')
    plt.close()
    #plt.show()
    
#print(line_chart_specific_days(Timeliness.percent_exit_records_created_within_x_days,'exit_timeliness.png'))


