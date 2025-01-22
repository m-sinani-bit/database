import sqlite3
import os
import pandas as pd
import requests
import json
import csv
import mysql.connector
from mysql.connector import Error
import mysql.connector
import msal
import logging
import numpy as np


import measure_definitions as m

logging.basicConfig(filename='data_loading.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def database_initialization(server_config, database_name):
    conn = mysql.connector.connect(**server_config)
    cursor = conn.cursor()

    cursor.execute("SET foreign_key_checks = 0;")

    cursor.execute(f"DROP DATABASE IF EXISTS {database_name}")
    cursor.execute(f"CREATE DATABASE {database_name}")
    cursor.execute(f"USE {database_name}")
    
    print(f'Creating database: {database_name}')


    # Export Table
    cursor.execute('''CREATE TABLE Export
    (
        ExportID VARCHAR(32) PRIMARY KEY,
        SourceType INT,
        SourceID VARCHAR(32),
        SourceName VARCHAR(50),
        SourceContactFirst VARCHAR(50),
        SourceContactLast VARCHAR(50),
        SourceContactPhone VARCHAR(10),
        SourceContactExtension VARCHAR(5),
        SourceContactEmail VARCHAR(320),
        ExportDate DATETIME,
        ExportStartDate DATE,
        ExportEndDate DATE,
        SoftwareName VARCHAR(50),
        SoftwareVersion VARCHAR(50),
        CSVVersion VARCHAR(50),
        ExportPeriodType INT,
        ExportDirective INT,
        HashStatus INT,
        ImplementationID VARCHAR(200)
    )''')

    # Organization Table
    cursor.execute('''CREATE TABLE Organization
    (
        OrganizationID VARCHAR(32) PRIMARY KEY,
        OrganizationName VARCHAR(200),
        VictimServiceProvider INT,
        OrganizationCommonName VARCHAR(200),
        DateCreated DATETIME,
        DateUpdated DATETIME,
        UserID VARCHAR(32),
        DateDeleted DATETIME,
        ExportID VARCHAR(32),
        FOREIGN KEY (ExportID) REFERENCES Export(ExportID)
    )''')

    cursor.execute('''CREATE INDEX idx_Organization_ExportID ON Organization (ExportID)''')

    # Project Table
    cursor.execute('''CREATE TABLE Project
    (
        ProjectID VARCHAR(32) PRIMARY KEY,
        OrganizationID VARCHAR(32),
        ProjectName VARCHAR(200),
        ProjectCommonName VARCHAR(200),
        OperatingStartDate DATE,
        OperatingEndDate DATE,
        ContinuumProject INT,
        ProjectType INT,
        HousingType INT,
        RRHSubType INT,
        ResidentialAffiliation INT,
        TargetPopulation INT,
        HOPWAMedAssistedLivingFac INT,
        PITCount INT,
        DateCreated DATETIME,
        DateUpdated DATETIME,
        UserID VARCHAR(32),
        DateDeleted DATETIME,
        ExportID VARCHAR(32),
        FOREIGN KEY (ExportID) REFERENCES Export(ExportID),
        FOREIGN KEY (OrganizationID) REFERENCES Organization(OrganizationID)
    )''')

    cursor.execute('''CREATE INDEX idx_Project_ExportID ON Project (ExportID)''')
    cursor.execute('''CREATE INDEX idx_Project_OrganizationID ON Project (OrganizationID)''')

    # Affiliation Table
    cursor.execute('''CREATE TABLE Affiliation
    (
        AffiliationID VARCHAR(32) PRIMARY KEY,
        ProjectID VARCHAR(32),
        ResProjectID VARCHAR(32),
        DateCreated DATETIME,
        DateUpdated DATETIME,
        UserID VARCHAR(32),
        DateDeleted DATETIME,
        ExportID VARCHAR(32),
        FOREIGN KEY (ProjectID) REFERENCES Project(ProjectID),
        FOREIGN KEY (ExportID) REFERENCES Export(ExportID)
    )''')

    cursor.execute('''CREATE INDEX idx_Affiliation_ProjectID ON Affiliation (ProjectID)''')
    cursor.execute('''CREATE INDEX idx_Affiliation_ExportID ON Affiliation (ExportID)''')

    # Assessment Table
    cursor.execute('''CREATE TABLE Assessment
    (
        AssessmentID VARCHAR(32) PRIMARY KEY,
        EnrollmentID VARCHAR(32),
        PersonalID VARCHAR(32),
        AssessmentDate DATE,
        AssessmentLocation VARCHAR(250),
        AssessmentType INT,
        AssessmentLevel INT,
        PrioritizationStatus INT,
        DateCreated DATETIME,
        DateUpdated DATETIME,
        UserID VARCHAR(32),
        DateDeleted DATETIME,
        ExportID VARCHAR(32),
        ScreenId VARCHAR(32),
        ScreenName VARCHAR(255),
        FOREIGN KEY (ExportID) REFERENCES Export(ExportID)
    )''')

    # AssessmentQuestions Table    
    cursor.execute('''CREATE TABLE AssessmentQuestions
    (
        AssessmentQuestionID VARCHAR(50),
        AssessmentID VARCHAR(32),
        EnrollmentID VARCHAR(32),
        PersonalID VARCHAR(32),
        AssessmentQuestionGroup VARCHAR(250),
        AssessmentQuestionOrder INT,
        AssessmentQuestion VARCHAR(250),
        AssessmentAnswer VARCHAR(500),
        DateCreated DATETIME,
        DateUpdated DATETIME,
        UserID VARCHAR(32),
        DateDeleted DATETIME,
        ExportID VARCHAR(32),
        FOREIGN KEY (ExportID) REFERENCES Export(ExportID)
    )''')

    # AssessmentResults Table
    cursor.execute('''CREATE TABLE AssessmentResults
    (
        AssessmentResultID VARCHAR(50),
        AssessmentID VARCHAR(32),
        EnrollmentID VARCHAR(32),
        PersonalID VARCHAR(32),
        AssessmentResultType VARCHAR(250),
        AssessmentResult VARCHAR(250),
        DateCreated DATETIME,
        DateUpdated DATETIME,
        UserID VARCHAR(32),
        DateDeleted DATETIME,
        ExportID VARCHAR(255),
        FOREIGN KEY (ExportID) REFERENCES Export(ExportID)
    )''')

    # CEParticipation Table
    cursor.execute('''CREATE TABLE CEParticipation
    (
        CEParticipationID VARCHAR(32) PRIMARY KEY,
        ProjectID VARCHAR(32),
        AccessPoint INT,
        PreventionAssessment INT,
        CrisisAssessment INT,
        HousingAssessment INT,
        DirectServices INT,
        ReceivesReferrals INT,
        CEParticipationStatusStartDate DATE,
        CEParticipationStatusEndDate DATE,
        DateCreated DATETIME,
        DateUpdated DATETIME,
        UserID VARCHAR(32),
        DateDeleted DATETIME, 
        ExportID VARCHAR(32),
        FOREIGN KEY (ExportID) REFERENCES Export(ExportID),
        FOREIGN KEY (ProjectID) REFERENCES Project(ProjectID)
    )''')

    # Client Table
    cursor.execute('''CREATE TABLE Client
    (
        PersonalID VARCHAR(32) PRIMARY KEY,
        FirstName VARCHAR(50),
        MiddleName VARCHAR(50),
        LastName VARCHAR(50),
        NameSuffix VARCHAR(50),
        NameDataQuality INT,
        SSN VARCHAR(20),
        SSNDataQuality INT,
        DOB DATE,
        DOBDataQuality INT,
        AmIndAKNative INT,
        Asian INT,
        BlackAfAmerican INT,
        HispanicLatinaeo INT,
        MidEastNAfrican INT,
        NativeHIPacific INT,
        White INT,
        RaceNone INT,
        AdditionalRaceEthnicity VARCHAR(100),
        Woman INT,
        Man INT,
        NonBinary INT,
        CulturallySpecific INT,
        Transgender INT,
        Questioning INT,
        DifferentIdentity INT,
        GenderNone INT,
        DifferentIdentityText VARCHAR(100),
        VeteranStatus INT,
        YearEnteredService INT,
        YearSeparated INT,
        WorldWarII INT, 
        KoreanWar INT,
        VietnamWar INT,
        DesertStorm INT,
        AfghanistanOEF INT,
        IraqOIF INT,
        IraqOND INT,
        OtherTheater INT,
        MilitaryBranch INT,
        DischargeStatus INT,
        DateCreated DATETIME,
        DateUpdated DATETIME,
        UserID VARCHAR(32),
        DateDeleted DATETIME,
        ExportID VARCHAR(32),
        FOREIGN KEY (ExportID) REFERENCES Export(ExportID)
    )''')

    # CurrentLivingSituation Table
    cursor.execute('''CREATE TABLE CurrentLivingSituation
    (
        CurrentLivingSitID VARCHAR(32) PRIMARY KEY,
        EnrollmentID VARCHAR(32),
        PersonalID VARCHAR(32),
        InformationDate DATE,
        CurrentLivingSituation INT,
        CLSSubsidyType INT,
        VerifiedBy VARCHAR(100),
        LeaveSituation14Days INT,
        SubsequentResidence INT,
        ResourcesToObtain INT,
        LeaseOwn60Day INT,
        MovedTwoOrMore INT,
        LocationDetails VARCHAR(250),
        DateCreated DATETIME,
        DateUpdated DATETIME,
        UserID VARCHAR(32),
        DateDeleted DATETIME,
        ExportID VARCHAR(32),
        FOREIGN KEY (ExportID) REFERENCES Export(ExportID)
    )''')

    # Disabilities Table
    cursor.execute('''CREATE TABLE Disabilities
    (
        DisabilitiesID VARCHAR(32) PRIMARY KEY,
        EnrollmentID VARCHAR(32),
        PersonalID VARCHAR(32),
        InformationDate DATE,
        DisabilityType INT,
        DisabilityResponse INT,
        IndefiniteAndImpairs INT,
        TCellCountAvailable INT,
        TCellCount INT,
        TCellSource INT,
        ViralLoadAvailable INT,
        ViralLoad INT,
        ViralLoadSource INT,
        AntiRetroviral INT,
        DataCollectionStage INT,
        DateCreated DATETIME,
        DateUpdated DATETIME,
        UserID VARCHAR(32),
        DateDeleted DATETIME,
        ExportID VARCHAR(32),
        FOREIGN KEY (ExportID) REFERENCES Export(ExportID)
    )''')

    # EmploymentEducation Table
    cursor.execute('''CREATE TABLE EmploymentEducation
    (
        EmploymentEducationID VARCHAR(32) PRIMARY KEY,
        EnrollmentID VARCHAR(32),
        PersonalID VARCHAR(32),
        InformationDate DATE,
        LastGradeCompleted INT,
        SchoolStatus INT,
        Employed INT,
        EmploymentType INT,
        NotEmployedReason INT,
        DataCollectionStage INT,
        DateCreated DATETIME,
        DateUpdated DATETIME,
        UserID VARCHAR(32),
        DateDeleted DATETIME,
        ExportID VARCHAR(32),
        FOREIGN KEY (ExportID) REFERENCES Export(ExportID)
    )''')

    # Enrollment Table
    cursor.execute('''CREATE TABLE Enrollment
    (
        EnrollmentID VARCHAR(32) PRIMARY KEY,
        PersonalID VARCHAR(32),
        ProjectID VARCHAR(32),
        EntryDate DATE,
        HouseholdID VARCHAR(32),
        RelationshipToHoH INT,
        EnrollmentCoC VARCHAR(32),
        LivingSituation INT,
        RentalSubsidyType INT,
        LengthOfStay INT,
        LOSUnderThreshold INT,
        PreviousStreetESSH INT,
        DateToStreetESSH DATE,
        TimesHomelessPastThreeYears INT,
        MonthsHomelessPastThreeYears INT,
        DisablingCondition INT,
        DateOfEngagement DATE,
        MoveInDate DATE,
        DateOfPATHStatus DATE,
        ClientEnrolledInPATH INT,
        ReasonNotEnrolled INT,
        PercentAMI INT,
        ReferralSource INT,
        CountOutreachReferralApproaches INT,
        DateOfBCPStatus DATE,
        EligibleForRHY INT,
        ReasonNoServices INT,
        RunawayYouth INT,
        SexualOrientation INT,
        SexualOrientationOther VARCHAR(100),
        FormerWardChildWelfare INT,
        ChildWelfareYears INT,
        ChildWelfareMonths INT,
        FormerWardJuvenileJustice INT,
        JuvenileJusticeYears INT,
        JuvenileJusticeMonths INT,
        UnemploymentFam INT,
        MentalHealthDisorderFam INT,
        PhysicalDisabilityFam INT,
        AlcoholDrugUseDisorderFam INT,
        InsufficientIncome INT,
        IncarceratedParent INT,
        VAMCStation VARCHAR(5),
        TargetScreenReqd INT,
        TimeToHousingLoss INT,
        AnnualPercentAMI INT,
        LiteralHomelessHistory INT,
        ClientLeaseholder INT,
        HOHLeaseholder INT,
        SubsidyAtRisk INT,
        EvictionHistory INT,
        CriminalRecord INT,
        IncarceratedAdult INT,
        PrisonDischarge INT,
        SexOffender INT,
        DisabledHoH INT,
        CurrentPregnant INT,
        SingleParent INT,
        DependentUnder6 INT,
        HH5Plus INT,
        CoCPrioritized INT,
        HPScreeningScore INT,
        ThresholdScore INT,
        TranslationNeeded INT,
        PreferredLanguage INT,
        PreferredLanguageDifferent VARCHAR(100),
        DateCreated DATETIME,
        DateUpdated DATETIME,
        UserID VARCHAR(32),
        DateDeleted DATETIME,
        ExportID VARCHAR(32),
        FOREIGN KEY (ProjectID) REFERENCES Project(ProjectID),
        FOREIGN KEY (EnrollmentCoC) REFERENCES Project(ProjectID),
        FOREIGN KEY (ExportID) REFERENCES Export(ExportID)
    )''')

    # Event Table
    cursor.execute('''CREATE TABLE Event
    (
        EventID VARCHAR(32) PRIMARY KEY,
        EnrollmentID VARCHAR(32),
        PersonalID VARCHAR(32),
        EventDate DATE,
        Event INT,
        ProbSolDivRRResult INT,
        ReferralCaseManageAfter INT,
        LocationCrisisOrPHHousing VARCHAR(250),
        ReferralResult INT,
        ResultDate DATE,
        DateCreated DATETIME,
        DateUpdated DATETIME,
        UserID VARCHAR(32),
        DateDeleted DATETIME,
        ExportID VARCHAR(32),
        FOREIGN KEY (ExportID) REFERENCES Export(ExportID)
    )''')

    # Exit Table
    cursor.execute('''CREATE TABLE `Exit`
    (
        ExitID VARCHAR(32) PRIMARY KEY,
        EnrollmentID VARCHAR(32),
        PersonalID VARCHAR(32),
        ExitDate DATE,
        Destination INT,
        DestinationSubsidyType INT,
        OtherDestination VARCHAR(50),
        HousingAssessment INT,
        SubsidyInformation INT,
        ProjectCompletionStatus INT,
        EarlyExitReason INT,
        ExchangeForSex INT,
        ExchangeForSexPastThreeMonths INT,
        CountOfExchangeForSex INT,
        AskedOrForcedToExchangeForSex INT,
        AskedOrForcedToExchangeForSexPastThreeMonths INT,
        WorkplaceViolenceThreats INT,
        WorkplacePromiseDifference INT,
        CoercedToContinueWork INT,
        LaborExploitPastThreeMonths INT,
        CounselingReceived INT,
        IndividualCounseling INT,
        FamilyCounseling INT,
        GroupCounseling INT,
        SessionCountAtExit INT,
        PostExitCounselingPlan INT,
        SessionsInPlan INT,
        DestinationSafeClient INT,
        DestinationSafeWorker INT,
        PosAdultConnections INT,
        PosPeerConnections INT,
        PosCommunityConnections INT,
        AftercareDate DATE,
        AftercareProvided INT,
        EmailSocialMedia INT,
        Telephone INT,
        InPersonIndividual INT,
        InPersonGroup INT,
        CMExitReason INT,
        DateCreated DATETIME,
        DateUpdated DATETIME,
        UserID VARCHAR(32),
        DateDeleted DATETIME,
        ExportID VARCHAR(32),
        FOREIGN KEY (ExportID) REFERENCES Export(ExportID)
    )''')

    # Funder Table
    cursor.execute('''CREATE TABLE Funder
    (
        FunderID VARCHAR(32) PRIMARY KEY,
        ProjectID VARCHAR(32),
        Funder INT,
        OtherFunder VARCHAR(100),
        GrantID VARCHAR(100),
        StartDate DATE,
        EndDate DATE,
        DateCreated DATETIME,
        DateUpdated DATETIME,
        UserID VARCHAR(32),
        DateDeleted DATETIME,
        ExportID VARCHAR(32),
        FOREIGN KEY (ExportID) REFERENCES Export(ExportID),
        FOREIGN KEY (ProjectID) REFERENCES Project(ProjectID)
    )''')

    # HMISParticipation Table
    cursor.execute('''CREATE TABLE HMISParticipation
    (
        HMISParticipationID VARCHAR(32) PRIMARY KEY,
        ProjectID VARCHAR(32),
        HMISParticipationType INT,
        HMISParticipationStatusStartDate DATE,
        HMISParticipationStatusEndDate DATE,
        DateCreated DATETIME,
        DateUpdated DATETIME,
        UserID VARCHAR(32),
        DateDeleted DATETIME,
        ExportID VARCHAR(32),
        FOREIGN KEY (ExportID) REFERENCES Export(ExportID),
        FOREIGN KEY (ProjectID) REFERENCES Project(ProjectID)
    )''')

    # HealthAndDV Table
    cursor.execute('''CREATE TABLE HealthAndDV
    (
        HealthAndDVID VARCHAR(32) PRIMARY KEY,
        EnrollmentID VARCHAR(32),
        PersonalID VARCHAR(32),
        InformationDate DATE,
        DomesticViolenceSurvivor INT,
        WhenOccurred INT,
        CurrentlyFleeing INT,
        GeneralHealthStatus INT,
        DentalHealthStatus INT,
        MentalHealthStatus INT,
        PregnancyStatus INT,
        DueDate DATE,
        DataCollectionStage INT,
        DateCreated DATETIME,
        DateUpdated DATETIME,
        UserID VARCHAR(32),
        DateDeleted DATETIME,
        ExportID VARCHAR(32),
        FOREIGN KEY (ExportID) REFERENCES Export(ExportID)
    )''')

    # IncomeBenefits Table
    cursor.execute('''CREATE TABLE IncomeBenefits 
    (
        IncomeBenefitsID VARCHAR(32) PRIMARY KEY,
        EnrollmentID VARCHAR(32),
        PersonalID VARCHAR(32),
        InformationDate DATE,
        IncomeFromAnySource INT,
        TotalMonthlyIncome FLOAT SIGNED,
        Earned INT,
        EarnedAmount FLOAT SIGNED,
        Unemployment INT,
        UnemploymentAmount FLOAT SIGNED,
        SSI INT,
        SSIAmount FLOAT SIGNED,
        SSDI INT,
        SSDIAmount FLOAT SIGNED,
        VADisabilityService INT,
        VADisabilityServiceAmount FLOAT SIGNED,
        VADisabilityNonService INT,
        VADisabilityNonServiceAmount FLOAT SIGNED,
        PrivateDisability INT,
        PrivateDisabilityAmount FLOAT SIGNED,
        WorkersComp INT,
        WorkersCompAmount FLOAT SIGNED,
        TANF INT,
        TANFAmount FLOAT SIGNED,
        GA INT,
        GAAmount FLOAT SIGNED,
        SocSecRetirement INT,
        SocSecRetirementAmount FLOAT SIGNED,
        Pension INT,
        PensionAmount FLOAT SIGNED,
        ChildSupport INT,
        ChildSupportAmount FLOAT SIGNED,
        Alimony INT,
        AlimonyAmount FLOAT SIGNED,
        OtherIncomeSource INT,
        OtherIncomeAmount FLOAT SIGNED,
        OtherIncomeSourceIdentify VARCHAR(50),
        BenefitsFromAnySource INT,
        SNAP INT,
        WIC INT,
        TANFChildCare INT,
        TANFTransportation INT,
        OtherTANF INT,
        OtherBenefitsSource INT,
        OtherBenefitsSourceIdentify VARCHAR(50),
        InsuranceFromAnySource INT,
        Medicaid INT,
        NoMedicaidReason INT,
        Medicare INT,
        NoMedicareReason INT,
        SCHIP INT,
        NoSCHIPReason INT,
        VHAServices INT,
        NoVHAReason INT,
        EmployerProvided INT,
        NoEmployerProvidedReason INT,
        COBRA INT,
        NoCOBRAReason INT,
        PrivatePay INT,
        NoPrivatePayReason INT,
        StateHealthIns INT,
        NoStateHealthInsReason INT,
        IndianHealthServices INT,
        NoIndianHealthServicesReason INT,
        OtherInsurance INT,
        OtherInsuranceIdentify VARCHAR(50),
        ADAP INT,
        NoADAPReason INT,
        RyanWhiteMedDent INT,
        NoRyanWhiteReason INT,
        ConnectionWithSOAR INT,
        DataCollectionStage INT,
        DateCreated DATETIME,
        DateUpdated DATETIME,
        UserID VARCHAR(32),
        DateDeleted DATETIME,
        ExportID VARCHAR(32),
        FOREIGN KEY (ExportID) REFERENCES Export(ExportID)
    )''')

    # ProjectCoC Table
    cursor.execute('''CREATE TABLE ProjectCoC
    (
        ProjectCoCID VARCHAR(32) PRIMARY KEY,
        ProjectID VARCHAR(32),
        CoCCode VARCHAR(6),
        Geocode VARCHAR(6),
        Address1 VARCHAR(100),
        Address2 VARCHAR(100),
        City VARCHAR(50),
        State VARCHAR(2),
        ZIP VARCHAR(10),
        GeographyType INT,
        DateCreated DATETIME,
        DateUpdated DATETIME,
        UserID VARCHAR(32),
        DateDeleted DATETIME,
        ExportID VARCHAR(32),
        FOREIGN KEY (ExportID) REFERENCES Export(ExportID),
        FOREIGN KEY (ProjectID) REFERENCES Project(ProjectID)
    )''')


    cursor.execute('''CREATE INDEX idx_ProjectCoC_ExportID ON ProjectCoC (ExportID)''')
    cursor.execute('''CREATE INDEX idx_ProjectCoC_ProjectID ON ProjectCoC (ProjectID)''')
    cursor.execute('''CREATE INDEX idx_ProjectCoC_CoCCode ON ProjectCoC (CoCCode)''')

    # Inventory table
    cursor.execute('''CREATE TABLE Inventory
    (
        InventoryID VARCHAR(32) PRIMARY KEY,
        ProjectID VARCHAR(32),
        CoCCode VARCHAR(6),
        HouseholdType INT,
        Availability INT,
        UnitInventory INT,
        BedInventory INT,
        CHVetBedInventory INT,
        YouthVetBedInventory INT,
        VetBedInventory INT,
        CHYouthBedInventory INT,
        YouthBedInventory INT,
        CHBedInventory INT,
        OtherBedInventory INT,
        ESBedType INT,
        InventoryStartDate DATE,
        InventoryEndDate DATE,
        DateCreated DATETIME,
        DateUpdated DATETIME,
        UserID VARCHAR(32),
        DateDeleted DATETIME,
        ExportID VARCHAR(32),
        FOREIGN KEY (ProjectID) REFERENCES Project(ProjectID),
        FOREIGN KEY (CoCCode) REFERENCES ProjectCoC(CoCCode),
        FOREIGN KEY (ExportID) REFERENCES Export(ExportID)
    )''')
    
    cursor.execute('''CREATE INDEX idx_Inventory_ProjectID ON Inventory (ProjectID)''')
    cursor.execute('''CREATE INDEX idx_Inventory_CoCCode ON Inventory (CoCCode)''')
    cursor.execute('''CREATE INDEX idx_Inventory_ExportID ON Inventory (ExportID)''')

   
    # Services Table
    cursor.execute('''CREATE TABLE Services
    (
        ServicesID VARCHAR(32) PRIMARY KEY,
        EnrollmentID VARCHAR(32),
        PersonalID VARCHAR(32),
        DateProvided DATE,
        RecordType INT,
        TypeProvided INT,
        OtherTypeProvided VARCHAR(50),
        MovingOnOtherType VARCHAR(50),
        SubTypeProvided INT,
        FAAmount FLOAT,
        FAStartDate DATE,
        FAEndDate DATE,
        ReferralOutcome INT,
        DateCreated DATETIME,
        DateUpdated DATETIME,
        UserID VARCHAR(32),
        DateDeleted DATETIME,
        ExportID VARCHAR(32),
        ServiceItemId VARCHAR(32),
        ServiceEndDate DATE,
        FOREIGN KEY (ExportID) REFERENCES Export(ExportID)
    )''')

    # User Table
    cursor.execute('''CREATE TABLE User
    (
        UserID VARCHAR(32) PRIMARY KEY,
        UserFirstName VARCHAR(1024),
        UserLastName VARCHAR(50),
        UserPhone VARCHAR(50),
        UserExtension VARCHAR(1024),
        UserEmail VARCHAR(320),
        DateCreated DATETIME,   
        DateUpdated DATETIME,
        DateDeleted DATETIME,
        ExportID VARCHAR(32),
        FOREIGN KEY (ExportID) REFERENCES Export(ExportID)
    )''')

    # YouthEducationStatus Table
    cursor.execute('''CREATE TABLE YouthEducationStatus
    (
        YouthEducationStatusID VARCHAR(32) PRIMARY KEY,
        EnrollmentID VARCHAR(32),
        PersonalID VARCHAR(32),
        InformationDate DATE,
        CurrentSchoolAttend INT,
        MostRecentEdStatus INT,
        CurrentEdStatus INT,
        DataCollectionStage INT,
        DateCreated DATETIME,
        DateUpdated DATETIME,
        UserID VARCHAR(32),
        DateDeleted DATETIME,
        ExportID VARCHAR(32),
        FOREIGN KEY (ExportID) REFERENCES Export(ExportID)
    )''')

    # CustomClientService Table
    cursor.execute('''CREATE TABLE CustomClientServices
    (
        CustomClientServiceID VARCHAR(32),
        ClientID VARCHAR(32),
        ClientProgramID VARCHAR(32),
        AgencyID VARCHAR(32),
        ServiceItemID VARCHAR(32),
        ServiceItemName VARCHAR(255),
        StartDate DATE,
        EndDate DATE,
        UserID VARCHAR(32),
        AddedDate DATETIME,
        LastUpdated DATETIME,
        Private INT,
        DepartmentID VARCHAR(32),
        UserUpdatedID VARCHAR(32),
        Deleted VARCHAR(32),
        AgencyDeletedID VARCHAR(32),
        FOREIGN KEY (ClientID) REFERENCES Client(PersonalID)
    )''')

    # CustomClientServiceAttendance Table
    cursor.execute('''CREATE TABLE CustomClientServiceAttendance
    (
        CustomClientServiceAttendanceID VARCHAR(32) ,
        CustomClientServiceID VARCHAR(32),
        Date DATE,
        time TIME,
        UserID VARCHAR(32), 
        AddedDate DATETIME,
        LastUpdated DATETIME,
        UserUpdatedID VARCHAR(32)       
    )''')

    # CustomClientServiceExpense Table
    cursor.execute('''CREATE TABLE CustomClientServiceExpense
    (  
        CustomClientServiceExpenseID VARCHAR(32),
        CustomClientServiceID VARCHAR(32),
        ServiceItemID VARCHAR(32),
        FundingID VARCHAR(32),
        Date DATE,
        Amount VARCHAR(32),
        CheckNumber VARCHAR(32),
        Vendor VARCHAR(255),
        Notes VARCHAR(2048),
        AttendanceID VARCHAR(32),
        AddedDate DATETIME,
        LastUpdated DATETIME,
        UserUpdatedID VARCHAR(32)
    )''')
    
    
    cursor.execute('''CREATE TABLE AdditionalInformation
                (PersonalID VARCHAR(32),
            EnrollmentID VARCHAR(32),
            HouseholdID VARCHAR(32),
            UniqueIdentifier VARCHAR(32),
            CaseManager VARCHAR(1024),
            StaffHomeAgency VARCHAR(1024),
            StaffActiveStatus VARCHAR(32),
	    ChronicallyHomeless VARCHAR(32))''')

    
    cursor.execute('''CREATE TABLE KPIAssessments
                (PersonalID VARCHAR(32),
            EnrollmentID VARCHAR(32),
            AssessmentName VARCHAR(1024),
            AssessmentID VARCHAR(32),
            AssessmentDate DATETIME)''')
    
    cursor.execute('''CREATE TABLE KPIDocuments
                (PersonalID VARCHAR(32),
            EnrollmentID VARCHAR(32),
            FileName VARCHAR(1024),
            FileDate DATETIME)''')
    
    cursor.execute("SET foreign_key_checks = 1;")

    conn.commit()
    conn.close()


def load_data_from_csv(server_config, database_name, folder_name):
    conn = mysql.connector.connect(**server_config)
    cursor = conn.cursor()

    cursor.execute(f"USE `{database_name}`")
    # Disable foreign key checks
    cursor.execute("SET foreign_key_checks = 0;")
    
    logging.info(f"Loading data into database: {database_name}")
    print(f"Loading data into database: {database_name}")
    
    # Get the list of tables in the current database
    cursor.execute("SHOW TABLES;")
    tables = cursor.fetchall()
    table_names = [table[0] for table in tables]

    # Iterate through the tables and load data from CSV files with the same names
    for table_name in table_names:
        csv_filename = os.path.join(folder_name, table_name + ".csv")
        
        if os.path.exists(csv_filename):
            logging.info(f"Database: {database_name}. Loading data into table: {table_name}")
            print(f"Database: {database_name}. Loading data into table: {table_name}")

            try:
                # Load data into a DataFrame
                df = pd.read_csv(csv_filename, low_memory=False)
                if table_name=='AdditionalInformation':
                    df.rename(columns={'Clients Client ID': 'PersonalID','Enrollments Enrollment ID':'EnrollmentID','Enrollments Household ID':'HouseholdID','Clients Unique Identifier':'UniqueIdentifier','Enrollments Assigned Staff':'CaseManager', 'Enrollments Assigned Staff Home Agency':'StaffHomeAgency','Enrollments Active in Project':'StaffActiveStatus'},inplace=True) 
                    df["ChronicallyHomeless"] = 0  #Add ChronicallyHomeless column to csv
                    df.to_csv(csv_filename,index=False)
                if table_name=='KPIAssessments':
                    df.rename(columns={'Clients Client ID': 'PersonalID','Enrollments Enrollment ID':'EnrollmentID','Client Assessments Name':'AssessmentName', 'Client Assessments Assessment ID':'AssessmentID','Client Assessments Assessment Date':'AssessmentDate'},inplace=True) 
                    df.to_csv(csv_filename,index=False)
                if table_name=='KPIDocuments':
                    df.rename(columns={'Clients Client ID': 'PersonalID','Enrollments Enrollment ID':'EnrollmentID','Clients Client File Name':'FileName', 'Clients Client File Date Created Date':'FileDate'},inplace=True) 
                    df.to_csv(csv_filename,index=False)
                # Replace NaN with None
                df = df.astype(object).where(pd.notnull(df), None)
                logging.info(f"Data types for table {table_name}: {df.dtypes}")

                columns = ", ".join(df.columns)
                placeholders = ", ".join(["%s"] * len(df.columns))

                # Prepare bulk insert query
                if table_name.lower() == "exit":
                    table_name = f"`{table_name}`"

                # Prepare bulk insert query
                insert_stmt = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                
                # Convert DataFrame to list of tuples
                data = df.to_records(index=False).tolist()
                
                # Execute bulk insert in chunks
                chunk_size = 10000  # Adjust chunk size as necessary
                total_chunks = (len(data) + chunk_size - 1) // chunk_size
                for i in range(0, len(data), chunk_size):
                    chunk = data[i:i+chunk_size]
                    try:
                        cursor.executemany(insert_stmt, chunk)
                        conn.commit()
                        logging.info(f"Database: {database_name}. Loaded chunk {i//chunk_size + 1} of {total_chunks} into {table_name}")
                        print(f"Database: {database_name}. Loaded chunk {i//chunk_size + 1} of {total_chunks} into {table_name}")                    
                    except mysql.connector.Error as e:
                        logging.error(f"Database: {database_name}. Error loading chunk into {table_name}: {e}")
                        print(f"Database: {database_name}. Error loading chunk into {table_name}: {e}")

                logging.info(f"Database: {database_name}. Loaded data into {table_name} from {csv_filename}")
            except mysql.connector.Error as e:
                logging.error(f"Database: {database_name}. Error loading data into {table_name} from {csv_filename}: {e}")
                print(f"Database: {database_name}. Error loading data into {table_name} from {csv_filename}: {e}")

    # Re-enable foreign key checks
    cursor.execute("SET foreign_key_checks = 1;")
    
    # Commit changes and close the database
    conn.commit()
    conn.close()
    logging.info(f"Finished loading data into database: {database_name}")
    print(f"Finished loading data into database: {database_name}")
 

def append_db_name_to_id_columns(server_config, database_name):
    conn = mysql.connector.connect(**server_config)
    cursor = conn.cursor()

    cursor.execute(f"USE `{database_name}`")
    
    cursor.execute("SET foreign_key_checks = 0;")

    cursor.execute("SHOW TABLES;")
    tables = cursor.fetchall()
       
    # Iterate through the tables and append the database name to "ID" columns
    for table in tables:
        table_name = table[0]
        cursor.execute(f"DESCRIBE `{table_name}`")
        columns = cursor.fetchall()

        for column in columns:
            column_name = column[0]

            if column_name.endswith("ID") or column_name.endswith("Id"):
                print(f"Appending ID to {database_name} {table_name} {column_name}")
                try:
                    cursor.execute(
                        f"UPDATE `{table_name}` SET `{column_name}` = CONCAT('{database_name}|', `{column_name}`);"
                    )
                    conn.commit()
                    print(f"Successfully updated {table_name}.{column_name}")
                except mysql.connector.Error as e:
                    logging.error(f"Error updating {table_name}.{column_name}: {e}")
                    print(f"Error updating {table_name}.{column_name}: {e}")

    cursor.execute("SET foreign_key_checks = 1;")

    conn.commit()
    conn.close()
    logging.info(f"Finished appending IDs for database: {database_name}")
    print(f"Finished appending IDs for database: {database_name}")



def load_path_master_list(server_config, output_database, path_program_database):
    conn = mysql.connector.connect(**server_config)
    cursor = conn.cursor()

    cursor.execute(f"DROP DATABASE IF EXISTS {path_program_database}")
    cursor.execute(f"CREATE DATABASE {path_program_database}")
    cursor.execute(f"USE {path_program_database}")
    
    print(f'Creating database: {path_program_database}')

    # Activation code
    # Define the client information for MSAL
    CLIENT_ID = '#########'  # Application (client) ID
    CLIENT_SECRET = '########'  # Client secret value
    TENANT_ID = '########'  # Directory (tenant) ID
    
    AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
    SCOPES = ["https://graph.microsoft.com/.default"]  
    GRAPH_API_ENDPOINT = "https://graph.microsoft.com/v1.0"

    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=AUTHORITY,
        client_credential=CLIENT_SECRET
    )

    # Acquire token for client
    result = app.acquire_token_for_client(scopes=SCOPES)

    if "access_token" in result:
        access_token = result["access_token"]
        site_id = "7df760ec-309f-4d3c-a2cf-eade67bdec18"
        list_id = "eeed1338-4966-40c9-bfd0-8d285863ebe0"
        
        # Correct URL format
        url = f"{GRAPH_API_ENDPOINT}/sites/{site_id}/lists/{list_id}/items?$expand=fields"

        headers = {
            "Authorization": f"Bearer {access_token}"
        }

        all_data = []  # Initialize list to collect all data

        while url:
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise an error for bad responses
                print("API request successful")
                
                data = response.json()  # Get the JSON response
                all_data.extend(data.get('value', []))  # Collect data

                # Check for next page
                url = data.get('@odata.nextLink')  # Check for pagination link
            except requests.exceptions.HTTPError as http_err:
                print(f"HTTP error occurred: {http_err}")
                break
            except Exception as err:
                print(f"Other error occurred: {err}")
                break

        print(f"Total programs pulled from SharePoint: {len(all_data)}")
    cursor.execute(f'''
        CREATE TABLE {path_program_database} (
            MergedProgramID VARCHAR(255) PRIMARY KEY,
            ProgramName VARCHAR(255),
            Region VARCHAR(255),
            Department VARCHAR(255),
            CoCCode VARCHAR(255),
            PrimaryDataSystem VARCHAR(255),
            SecondaryDataSystem VARCHAR(255),
            DataSystemID VARCHAR(255),
            DataSystemProgramName VARCHAR(255),
            DataSystemProgramType VARCHAR(255),
            DataSystemStatus VARCHAR(255),
            PATHProgramType VARCHAR(255),
            GrantCode VARCHAR(255),
            ContractTerm VARCHAR(255),
            ContractStatus VARCHAR(255),
            NotesQuestions TEXT
        )
    ''')
 
     # Insert SharePoint data into the database
    for item in all_data:
        fields = item.get('fields', {})
        cursor.execute(f'''
            INSERT INTO {path_program_database} (
                ProgramName, Region, Department,
                CoCCode, PrimaryDataSystem, SecondaryDataSystem,
                DataSystemID, DataSystemProgramName, DataSystemProgramType,
                DataSystemStatus, PATHProgramType, ContractStatus,
                NotesQuestions, GrantCode, MergedProgramID, ContractTerm
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (            
            fields.get('Title'),
            fields.get('field_1'),
            fields.get('field_2'),
            fields.get('field_3'),
            fields.get('field_4'),
            fields.get('field_5'),
            fields.get('field_6'),  # DataSystemID
            fields.get('field_7'),
            fields.get('field_8'),
            fields.get('field_9'),
            fields.get('field_10'),
            fields.get('field_11'),
            fields.get('field_12'),
            fields.get('GrantCode'),
            fields.get('Merged_x0020_Program_x0020_ID'),
            fields.get('Grant_x0028_s_x0029_Term0')
        ))

    conn.commit()

    print(f"Data has been successfully inserted into {path_program_database}.")

    cursor.execute(f"USE {output_database}")
    
    cursor.execute(f"DROP TABLE IF EXISTS {path_program_database}")
    cursor.execute(f'''
        CREATE TABLE {path_program_database} (
            MergedProgramID VARCHAR(255) PRIMARY KEY,
            ProgramName VARCHAR(255),
            Region VARCHAR(255),
            Department VARCHAR(255),
            CoCCode VARCHAR(255),
            PrimaryDataSystem VARCHAR(255),
            SecondaryDataSystem VARCHAR(255),
            DataSystemID VARCHAR(255),
            DataSystemProgramName VARCHAR(255),
            DataSystemProgramType VARCHAR(255),
            DataSystemStatus VARCHAR(255),
            PATHProgramType VARCHAR(255),
            GrantCode VARCHAR(255),
            ContractTerm VARCHAR(255),
            ContractStatus VARCHAR(255),
            NotesQuestions TEXT
        )
    ''')

    cursor.execute(f"INSERT INTO {path_program_database} SELECT * FROM {path_program_database}.{path_program_database} ")

    print(f"Successfully Added {path_program_database} to merged database")

    conn.commit()
    conn.close()
    

def update_move_in_dates(server_config, database_name, path_program_database):
    conn = mysql.connector.connect(**server_config)
    cursor = conn.cursor()

    cursor.execute(f"USE {database_name}")

    # Fetch unique HouseholdID-MoveInDate pairs
    query = f"""
    SELECT DISTINCT x.HouseholdID, x.MoveInDate 
    FROM {database_name}.Enrollment as x
    INNER JOIN {path_program_database}.PATHProgramMasterList as p
    ON x.ProjectID = p.MergedProgramID 
    WHERE x.MoveInDate IS NOT NULL 
    AND p.MergedProgramID IS NOT NULL;
    """
    cursor.execute(query)
    household_data = cursor.fetchall()

    # Log the number of unique pairs found
    logging.info(f"Found {len(household_data)} unique HouseholdID-MoveInDate pairs in {database_name}.")
    print(f"Found {len(household_data)} unique HouseholdID-MoveInDate pairs in {database_name}.")

    # Use a temporary table for batch update
    cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_update_dates")
    cursor.execute("""
    CREATE TEMPORARY TABLE temp_update_dates (
        HouseholdID VARCHAR(255),
        MoveInDate DATE,
        PRIMARY KEY (HouseholdID)
    )
    """)

    # Insert household_data into the temporary table
    insert_query = "INSERT IGNORE INTO temp_update_dates (HouseholdID, MoveInDate) VALUES (%s, %s)"
    cursor.executemany(insert_query, household_data)

    # Update the Enrollment table using the temporary table
    update_query = f"""
    UPDATE {database_name}.Enrollment e
    JOIN temp_update_dates t ON e.HouseholdID = t.HouseholdID
    SET e.MoveInDate = t.MoveInDate
    """
    cursor.execute(update_query)

    # Clean up the temporary table
    cursor.execute("DROP TEMPORARY TABLE temp_update_dates")

    logging.info(f"MoveInDate update completed for {database_name}.")
    print(f"MoveInDate update completed for {database_name}.")

    conn.commit()
    conn.close()

def update_engagement_dates(server_config, database_name, path_program_database):
    conn = mysql.connector.connect(**server_config)
    cursor = conn.cursor()

    cursor.execute(f"USE {database_name}")

    # Fetch unique HouseholdID-DateOfEngagement pairs
    query = f"""
    SELECT DISTINCT x.HouseholdID, x.DateOfEngagement 
    FROM {database_name}.Enrollment as x
    INNER JOIN {path_program_database}.PATHProgramMasterList as p
    ON x.ProjectID = p.MergedProgramID 
    WHERE x.DateOfEngagement IS NOT NULL 
    AND p.MergedProgramID IS NOT NULL;
    """
    cursor.execute(query)
    household_data = cursor.fetchall()

    # Log the number of unique pairs found
    logging.info(f"Found {len(household_data)} unique HouseholdID-DateOfEngagement pairs in {database_name}.")
    print(f"Found {len(household_data)} unique HouseholdID-DateOfEngagement pairs in {database_name}.")

    # Use a temporary table for batch update
    cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_update_engagement_dates")
    cursor.execute("""
    CREATE TEMPORARY TABLE temp_update_engagement_dates (
        HouseholdID VARCHAR(255),
        DateOfEngagement DATE,
        PRIMARY KEY (HouseholdID)
    )
    """)

    # Insert household_data into the temporary table
    insert_query = "INSERT IGNORE INTO temp_update_engagement_dates (HouseholdID, DateOfEngagement) VALUES (%s, %s)"
    cursor.executemany(insert_query, household_data)

    # Update the Enrollment table using the temporary table
    update_query = f"""
    UPDATE {database_name}.Enrollment e
    JOIN temp_update_engagement_dates t ON e.HouseholdID = t.HouseholdID
    SET e.DateOfEngagement = t.DateOfEngagement
    """
    cursor.execute(update_query)

    # Clean up the temporary table
    cursor.execute("DROP TEMPORARY TABLE temp_update_engagement_dates")

    logging.info(f"DateOfEngagement update completed for {database_name}.")
    print(f"DateOfEngagement update completed for {database_name}.")

    conn.commit()
    conn.close()


def ch_fill_in(server_config, database_name, path_program_database):
    logging.basicConfig(filename='data_loading.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    
    conn = mysql.connector.connect(**server_config)
    cursor = conn.cursor()

    cursor.execute(f"USE {database_name}")

    query = f'''
        UPDATE AdditionalInformation ai
        JOIN (
            SELECT DISTINCT
                e.PersonalID, e.EnrollmentID
            FROM Enrollment e
            LEFT JOIN {database_name}.`Exit` x ON e.EnrollmentID = x.EnrollmentID
            INNER JOIN {path_program_database}.PATHProgramMasterList p ON e.ProjectID = p.MergedProgramID
            LEFT JOIN {database_name}.Project pr ON e.ProjectID = pr.ProjectID
            LEFT JOIN {database_name}.Client c ON e.PersonalID = c.PersonalID
            WHERE (
                (
                    (e.RelationshipToHoH = 1 OR DATEDIFF(e.EntryDate, c.DOB) >= 6750) AND
                    e.DisablingCondition NOT IN (0, 8, 9, 99) AND 
                    pr.ProjectType IN (0, 1, 4, 8) AND 
                    (DATEDIFF(e.EntryDate, e.DateToStreetESSH) >= 365)
                ) OR (
                    (e.RelationshipToHoH = 1 OR DATEDIFF(e.EntryDate, c.DOB) >= 6750) AND 
                    e.DisablingCondition NOT IN (0, 8, 9, 99) AND 
                    pr.ProjectType IN (0, 1, 4, 8) AND 
                    e.DateToStreetESSH IS NULL AND 
                    e.TimesHomelessPastThreeYears = 4 AND 
                    e.MonthsHomelessPastThreeYears >= 112
                ) OR (
                    (e.RelationshipToHoH = 1 OR DATEDIFF(e.EntryDate, c.DOB) >= 6750) AND 
                    e.DisablingCondition NOT IN (0, 8, 9, 99) AND 
                    pr.ProjectType IN (0, 1, 4, 8) AND 
                    (DATEDIFF(e.EntryDate, e.DateToStreetESSH) < 365) AND 
                    e.TimesHomelessPastThreeYears = 4 AND 
                    e.MonthsHomelessPastThreeYears >= 112
                ) OR (
                    (e.RelationshipToHoH = 1 OR DATEDIFF(e.EntryDate, c.DOB) >= 6750) AND 
                    e.DisablingCondition NOT IN (0, 8, 9, 99) AND 
                    (e.LivingSituation BETWEEN 100 AND 199) AND 
                    (DATEDIFF(e.EntryDate, e.DateToStreetESSH) >= 365)
                ) OR (
                    (e.RelationshipToHoH = 1 OR DATEDIFF(e.EntryDate, c.DOB) >= 6750) AND 
                    e.DisablingCondition NOT IN (0, 8, 9, 99) AND 
                    (e.LivingSituation BETWEEN 100 AND 199) AND 
                    e.DateToStreetESSH IS NULL AND 
                    e.TimesHomelessPastThreeYears = 4 AND 
                    e.MonthsHomelessPastThreeYears >= 112
                ) OR (
                    (e.RelationshipToHoH = 1 OR DATEDIFF(e.EntryDate, c.DOB) >= 6750) AND 
                    e.DisablingCondition NOT IN (0, 8, 9, 99) AND 
                    (e.LivingSituation BETWEEN 100 AND 199) AND 
                    (DATEDIFF(e.EntryDate, e.DateToStreetESSH) < 365) AND 
                    e.TimesHomelessPastThreeYears = 4 AND 
                    e.MonthsHomelessPastThreeYears >= 112
                ) OR (
                    (e.RelationshipToHoH = 1 OR DATEDIFF(e.EntryDate, c.DOB) >= 6750) AND 
                    e.DisablingCondition NOT IN (0, 8, 9, 99) AND 
                    (e.LivingSituation BETWEEN 200 AND 299) AND 
                    e.LOSUnderThreshold = 1 AND
                    e.PreviousStreetESSH = 1 AND
                    (DATEDIFF(e.EntryDate, e.DateToStreetESSH) >= 365)
                ) OR (
                    (e.RelationshipToHoH = 1 OR DATEDIFF(e.EntryDate, c.DOB) >= 6750) AND 
                    e.DisablingCondition NOT IN (0, 8, 9, 99) AND 
                    (e.LivingSituation BETWEEN 200 AND 299) AND 
                    e.LOSUnderThreshold = 1 AND
                    e.PreviousStreetESSH = 1 AND
                    e.DateToStreetESSH IS NULL AND 
                    e.TimesHomelessPastThreeYears = 4 AND 
                    e.MonthsHomelessPastThreeYears >= 112
                ) OR (
                    (e.RelationshipToHoH = 1 OR DATEDIFF(e.EntryDate, c.DOB) >= 6750) AND 
                    e.DisablingCondition NOT IN (0, 8, 9, 99) AND 
                    (e.LivingSituation BETWEEN 200 AND 299) AND 
                    e.LOSUnderThreshold = 1 AND
                    e.PreviousStreetESSH = 1 AND
                    (DATEDIFF(e.EntryDate, e.DateToStreetESSH) < 365) AND 
                    e.TimesHomelessPastThreeYears = 4 AND 
                    e.MonthsHomelessPastThreeYears >= 112
                ) OR (
                    (e.RelationshipToHoH = 1 OR DATEDIFF(e.EntryDate, c.DOB) >= 6750) AND 
                    e.DisablingCondition NOT IN (0, 8, 9, 99) AND 
                    ((e.LivingSituation BETWEEN 300 AND 499) OR (e.LivingSituation BETWEEN 0 AND 99)) AND 
                    e.LOSUnderThreshold = 1 AND
                    e.PreviousStreetESSH = 1 AND
                    (DATEDIFF(e.EntryDate, e.DateToStreetESSH) >= 365)
                ) OR (
                    (e.RelationshipToHoH = 1 OR DATEDIFF(e.EntryDate, c.DOB) >= 6750) AND 
                    e.DisablingCondition NOT IN (0, 8, 9, 99) AND 
                    ((e.LivingSituation BETWEEN 300 AND 499) OR (e.LivingSituation BETWEEN 0 AND 99)) AND 
                    e.LOSUnderThreshold = 1 AND
                    e.PreviousStreetESSH = 1 AND
                    e.DateToStreetESSH IS NULL AND 
                    e.TimesHomelessPastThreeYears = 4 AND 
                    e.MonthsHomelessPastThreeYears >= 112
                ) OR (
                    (e.RelationshipToHoH = 1 OR DATEDIFF(e.EntryDate, c.DOB) >= 6750) AND 
                    e.DisablingCondition NOT IN (0, 8, 9, 99) AND 
                    ((e.LivingSituation BETWEEN 300 AND 499) OR (e.LivingSituation BETWEEN 0 AND 99)) AND 
                    e.LOSUnderThreshold = 1 AND
                    e.PreviousStreetESSH = 1 AND
                    (DATEDIFF(e.EntryDate, e.DateToStreetESSH) < 365) AND 
                    e.TimesHomelessPastThreeYears = 4 AND 
                    e.MonthsHomelessPastThreeYears >= 112
                )
            )
        ) AS sub ON ai.EnrollmentID = sub.EnrollmentID AND ai.PersonalID = sub.PersonalID
        SET ai.ChronicallyHomeless = 1;
    '''

    cursor.execute(query)
    conn.commit()
    conn.close()

    logging.info(f"Updated ChronicallyHomeless status for all matching records in {database_name}.")
    print(f"Updated ChronicallyHomeless status for all matching records in {database_name}.")

def apply_chronically_homeless_to_household(server_config, database_name):
    logging.basicConfig(filename='data_loading.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    conn = mysql.connector.connect(**server_config)
    cursor = conn.cursor()

    cursor.execute(f"USE {database_name}")

    # Update ChronicallyHomeless status for others in the same HouseholdID
    query = '''
        UPDATE AdditionalInformation ai1
        JOIN (
            SELECT DISTINCT HouseholdID
            FROM AdditionalInformation
            WHERE ChronicallyHomeless = 1 AND HouseholdID IS NOT NULL
        ) ai2 ON ai1.HouseholdID = ai2.HouseholdID
        SET ai1.ChronicallyHomeless = 1
        WHERE ai1.ChronicallyHomeless = 0
    '''
    
    cursor.execute(query)
    affected_rows = cursor.rowcount

    conn.commit()
    conn.close()

    logging.info(f"Finished adding ChronicallyHomeless status for all members of household in {database_name}..")
    print(f"Finished adding ChronicallyHomeless status for all members of household in {database_name}.")

def update_SD_entry_dates(server_config, database_name, path_program_database_name):
    logging.basicConfig(filename='data_loading.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    conn = mysql.connector.connect(**server_config)
    cursor = conn.cursor()


    cursor.execute(f"USE {database_name}")

    # Delete entries with conditions
    query = f"""
    DELETE e FROM {database_name}.Enrollment e
    WHERE EXISTS (
        SELECT 1
        FROM {path_program_database_name}.PATHProgramMasterList p
        WHERE e.ProjectID = p.MergedProgramID
        AND p.Region = "San Diego County"
        AND p.DataSystemProgramType IN ('PH - Housing with Services (no disability required for entry)', 'PH - Permanent Supportive Housing (disability required for entry)')
    )
    AND e.MoveInDate IS NULL
    """
    
    cursor.execute(query)
    affected_rows = cursor.rowcount

    conn.commit()
    conn.close()

    logging.info(f"Deleted {affected_rows} records from Enrollment in {database_name}.")
    print(f"Deleted {affected_rows} records from Enrollment in {database_name}.")

def attach_and_merge_data(server_config, output_database_name, database_name):
    conn = mysql.connector.connect(**server_config)
    cursor = conn.cursor()

    # Ensure the output database exists
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {output_database_name}")
    cursor.execute(f"USE {output_database_name}")

    # Disable foreign key checks before merging
    cursor.execute("SET foreign_key_checks = 0;")

    # Table order: parent tables first
    table_order = [
        "Export",
        "Project",
        "Client",
        "Affiliation",
        "Assessment",
        "AssessmentQuestions",
        "AssessmentResults",
        "CEParticipation",
        "CurrentLivingSituation",
        "CustomClientServiceAttendance",
        "CustomClientServiceExpense",
        "CustomClientServices",
        "Disabilities",
        "EmploymentEducation",
        "Enrollment",
        "Event",
        "Exit",
        "Funder",
        "HMISParticipation",
        "HealthAndDV",
        "IncomeBenefits",
        "Inventory",
        "Organization",
        "ProjectCoC",
        "Services",
        "User",
        "YouthEducationStatus",
        "AdditionalInformation",
        "KPIDocuments",
        "KPIAssessments"
        
    ]

    # Attach and merge data into the output database
    for table in table_order:
        try:
            # Check if the table exists in the output database
            cursor.execute(f"SHOW TABLES LIKE '{table}';")
            result = cursor.fetchone()
            if not result:
                # Get the CREATE TABLE statement from the source database
                cursor.execute(f"SHOW CREATE TABLE {database_name}.{table};")
                create_table_stmt = cursor.fetchone()[1]
                # Create the table in the output database
                cursor.execute(create_table_stmt)

            # Insert data from the attached table into the merged table
            merge_query = f"""
            INSERT INTO {output_database_name}.{table}
            SELECT * FROM {database_name}.{table};
            """
            cursor.execute(merge_query)
            conn.commit()
            logging.info(f"Merged table {table} from {database_name} into {output_database_name}")
            print(f"Merged table {table} from {database_name} into {output_database_name}")
        except mysql.connector.Error as e:
            logging.error(f"Error merging table {table} from {database_name} into {output_database_name}: {e}")
            print(f"Error merging table {table} from {database_name} into {output_database_name}: {e}")

    # Re-enable foreign key checks after merging
    cursor.execute("SET foreign_key_checks = 1;")

    conn.close()
    


def import_agency_indicators(filename):

    indicator_dict = {}
    with open(filename, 'r') as f:
        csv_reader = csv.DictReader(f)

        for row in csv_reader:
            ProgramType = row['ProgramType']
            IndicatorCategory = row['IndicatorCategory']
            IndicatorName = row['IndicatorName']
            IndicatorFunction = getattr(m, row['IndicatorFunction'], None)
            IndicatorParameter = row['IndicatorParameter']
            ParameterArgument = row['ParameterArgument']   
            Format = row['Format']  
            Target = row['Target']      
            IndicatorDomain = row['IndicatorDomain']
            Definition = row['Definition']  
            IndicatorType = row['IndicatorType']
            IndicatorFooter = row['IndicatorFooter']      
            # convert string to number for arguement and target
            try:
                Target = float(Target)
                if Target > 1:
                    Target = int(Target)
                else:
                    Target = float(Target)
            except ValueError:
                pass

            try:
                ParameterArgument = float(ParameterArgument)
            except ValueError:
                try:
                    ParameterArgument = int(ParameterArgument)
                except ValueError:
                    pass
            
            
            ParameterDict = {}
            if IndicatorParameter:
                ParameterDict = {IndicatorParameter : ParameterArgument}

            if ProgramType in indicator_dict:
                if IndicatorCategory in indicator_dict[ProgramType]:
                    if IndicatorName in indicator_dict[ProgramType][IndicatorCategory]:
                        indicator_dict[ProgramType][IndicatorCategory][IndicatorName].update({
                            'IndicatorFunction': IndicatorFunction,
                            'IndicatorParameter': ParameterDict,
                            'Format': Format,
                            'Target': Target,
                            'IndicatorDomain':IndicatorDomain,
                            'Definition':Definition,
                            'IndicatorType':IndicatorType,
                            'IndicatorFooter':IndicatorFooter
                        })
                    else:
                        indicator_dict[ProgramType][IndicatorCategory][IndicatorName] = {
                            'IndicatorFunction': IndicatorFunction,
                            'IndicatorParameter': ParameterDict,
                            'Format': Format,
                            'Target': Target,
                            'IndicatorDomain':IndicatorDomain,
                            'Definition':Definition,
                            'IndicatorType':IndicatorType,
                            'IndicatorFooter':IndicatorFooter
                        }
                else:
                    indicator_dict[ProgramType][IndicatorCategory] = {IndicatorName: {
                        'IndicatorFunction': IndicatorFunction,
                        'IndicatorParameter': ParameterDict,
                        'Format': Format,
                        'Target': Target,
                        'IndicatorDomain':IndicatorDomain,
                        'Definition':Definition,
                        'IndicatorType':IndicatorType,
                        'IndicatorFooter':IndicatorFooter
                    }}
            else:
                indicator_dict[ProgramType] = {IndicatorCategory: {IndicatorName: {
                    'IndicatorFunction': IndicatorFunction,
                    'IndicatorParameter': ParameterDict,
                    'Format': Format,
                    'Target': Target,
                    'IndicatorDomain':IndicatorDomain,
                    'Definition':Definition,
                    'IndicatorType':IndicatorType,
                    'IndicatorFooter':IndicatorFooter
                }}}
            
    return indicator_dict

def import_glossary(filename):

    glossary_dict = {}
    with open(filename, 'r') as f:
        csv_reader = csv.DictReader(f)

        for row in csv_reader:
            Category = row['Category']
            Name = row['Name']
            Definition = row['Definition']

            if Category in glossary_dict:
                glossary_dict[Category][Name] = Definition
            else:
                glossary_dict[Category] = {Name: Definition}
            
    return glossary_dict

def download_raw_csv():
    CLIENT_ID = '##########'
    CLIENT_SECRET = '########'
    TENANT_ID = '########'
    AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
    SCOPES = ["https://graph.microsoft.com/.default"]
    GRAPH_API_ENDPOINT = "https://graph.microsoft.com/v1.0"

    # MSAL application
    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=AUTHORITY,
        client_credential=CLIENT_SECRET
    )

    # Acquire token for client
    result = app.acquire_token_for_client(scopes=SCOPES)

    if "access_token" in result:
        access_token = result["access_token"]
        drive_id = "b!AlQvkpfM-Um7b88PHosfeyAbLTbrddlNqTcLh2MekuK7BLLnlvscQJ_Xokn5c40j"
        local_path = "/home/pathdb/raw_csvs"  # Specify your local path for downloading files
        parent_folder_id = "01YEVPESDEFPKQHVC35BAZ5ANHSRTTDTFT"  # DB Raw CSV Files folder id

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Connection": "keep-alive"
        }

        # Function to download a file
        def download_file(download_url, file_path):
            response = requests.get(download_url)
            if response.status_code == 200:
                with open(file_path, "wb") as file:
                    file.write(response.content)
                print(f"Downloaded {file_path}")
            else:
                print(f"Failed to download {file_path}: {response.status_code} - {response.text}")

        # Recursive function to download all items in a folder while preserving structure within selected folder
        def download_folder(drive_id, folder_id, folder_path):
            url = f"{GRAPH_API_ENDPOINT}/drives/{drive_id}/items/{folder_id}/children"
            response = requests.get(url, headers=headers, timeout=60)
            
            if response.status_code != 200:
                print(f"Failed to list items in folder {folder_id}: {response.status_code} - {response.text}")
                return
            
            items = response.json().get("value", [])
            os.makedirs(folder_path, exist_ok=True)

            for item in items:
                item_name = item["name"]
                if "file" in item:  # It's a file
                    download_url = item["@microsoft.graph.downloadUrl"]
                    download_file(download_url, os.path.join(folder_path, item_name))
                elif "folder" in item:  # It's a subfolder
                    subfolder_path = os.path.join(folder_path, item_name)
                    download_folder(drive_id, item["id"], subfolder_path)  # Recursive call for subfolder

        # Function to list items in a folder and select one to drill down
        def select_folder(drive_id, folder_id):
            url = f"{GRAPH_API_ENDPOINT}/drives/{drive_id}/items/{folder_id}/children"
            response = requests.get(url, headers=headers, timeout=60)
            
            if response.status_code != 200:
                print(f"Failed to list items in folder {folder_id}: {response.status_code} - {response.text}")
                return None
            
            items = response.json().get("value", [])
            
            print("Folders available for download:")
            folder_options = {}
            for i, item in enumerate(items, start=1):
                if "folder" in item:
                    folder_options[i] = item
                    print(f"{i}: {item['name']}")

            choice = int(input("Enter the number of the folder you want to download: "))
            if choice in folder_options:
                selected_folder = folder_options[choice]
                return selected_folder['id'], selected_folder['name']
            else:
                print("Invalid selection.")
                return None

        # Start by listing and selecting a folder
        selected_folder_id, selected_folder_name = select_folder(drive_id, parent_folder_id)
        if selected_folder_id:
            # Begin download process, starting directly in local_path (excludes selected folder name in structure)
            download_folder(drive_id, selected_folder_id, local_path)
        else:
            print("No folder selected, exiting.")

    else:
        print("Failed to acquire access token:", result.get("error"), result.get("error_description"))
