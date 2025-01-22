import os
import sqlite3
import pandas as pd
import json
import datetime
import textwrap
import numbers
from datetime import datetime, timedelta, date
import calendar
from pathlib import Path
import copy
import msal
import requests

from reportlab.pdfgen import canvas
from reportlab.platypus import SimpleDocTemplate, BaseDocTemplate, Paragraph, PageBreak, Image, Spacer, Table, TableStyle, Frame, Flowable, PageTemplate, NextPageTemplate
from reportlab.graphics.shapes import Drawing, Line
from reportlab.graphics.shapes import Drawing
from reportlab.graphics.shapes import Rect, Ellipse, Circle
from reportlab.graphics.charts.textlabels import Label

from reportlab.lib import utils

from reportlab.lib.enums import TA_LEFT, TA_RIGHT, TA_CENTER, TA_JUSTIFY
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.pagesizes import LETTER, inch, landscape
from reportlab.graphics.shapes import Line, LineShape, Drawing
from reportlab.lib import colors
from reportlab.lib.colors import Color, gray, black, green, blue, red, purple, lavender
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics
from reportlab.platypus.tableofcontents import TableOfContents
from reportlab.lib.colors import HexColor

import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter
from PIL import Image as IMG
import numpy as np

import measure_definitions as m
#import KPI_measures_2024 as k
import db_setup_functions as dsf
import report_formatting_server as rf


database_name = "PATHParticipantDB"
indicator_csv = "PATHIndicators.csv"
kpi_log_csv = "kpilog.csv"
glossary_csv = "glossary.csv"

report_folder = "completed_reports"

image_library_name = 'images'
chart_library_name = 'charts'
font_library_name = 'fonts'

image_directory = "images"
chart_directory = "charts"
font_directory = "fonts"


class ReportDocTemplate(BaseDocTemplate):
    def __init__(self, filename, **kw):
        self.allowSplitting = 0
        BaseDocTemplate.__init__(self, filename, **kw)
        template = PageTemplate('normal', [Frame(0,0,8.5*inch,11*inch,id='F1',
                                                  leftPadding=inch/2, rightPadding=inch/2, bottomPadding=inch/2, topPadding=inch/2)],pagesize=LETTER)
        self.addPageTemplates(template)

        template_landscape = PageTemplate('landscape', [Frame(0,0,8.5*inch,11*inch,id='F1',
                                                  leftPadding=inch/2, rightPadding=inch/2, bottomPadding=inch/2, topPadding=inch/2)],pagesize=landscape(LETTER))
        self.addPageTemplates(template_landscape)

    def afterFlowable(self, flowable):
        "Registers the Table of Contents entries"
        if flowable.__class__.__name__ == 'Paragraph':
            text = flowable.getPlainText()
            style = flowable.style.name
            if style == 'Page Header':
                self.notify('TOCEntry', (0,text,self.page))            
            if style == 'Program Header':
                self.notify('TOCEntry', (1,text,self.page))            
            if style == 'programsummarytitlePageStyle':
                self.notify('TOCEntry', (0,text,self.page))            

class ReportCanvas(canvas.Canvas):
    def __init__(self, *args, **kwargs):
        canvas.Canvas.__init__(self, *args, **kwargs)
        self.pages = []
        self.width, self.height = LETTER

    def showPage(self):
        self.pages.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        page_count = len(self.pages)
        for page in self.pages:
            self.__dict__.update(page)
            if (self._pageNumber > 1):
                self.draw_canvas(page_count)
            canvas.Canvas.showPage(self)
        canvas.Canvas.save(self)

    def draw_canvas(self, page_count):
        
        self.saveState()

        # header
        header_text = "People Assisting the Homeless"

        fontName = 'Montserrat'
        fontSize = 12

        self.setFont(fontName, fontSize)
        self.drawString(self.width-inch/2-(stringWidth(header_text, fontName=fontName, fontSize=fontSize)), self.height-inch/2 + fontSize/2, header_text)
        self.line(inch/2, self.height-inch/2, self.width-inch/2 ,self.height-inch/2)

        # footer
        page = "Page %s of %s" % (self._pageNumber, page_count)

        footer_text = report_name

        fontName = 'OpenSans'
        fontSize = 10

        self.setFont(fontName, fontSize)

        self.drawString(self.width-inch/2-(stringWidth(page, fontName=fontName, fontSize=fontSize)), inch/2 - fontSize, page)
        self.drawString(self.width-inch*8, inch/2 - fontSize, footer_text)

        self.line(inch/2, inch/2, self.width-inch/2 , inch/2)

        self.restoreState()
            
class QuarterlyReports:

    def __init__(self, report_type, division, fy_name, cadence_name, start_date, end_date, fy_start_date):
        self.division_name = division
        self.styleSheet = getSampleStyleSheet()
        self.elements = []

        self.master_dict = m.all_programs_dict()
        self.agency_indicator_functions_dict = dsf.import_agency_indicators(indicator_csv)
        #self.kpi_dict = dsf.import_kpi(kpi_log_csv)
        self.glossary_dict = dsf.import_glossary(glossary_csv)

        self.start_date = start_date
        self.end_date = end_date
        self.fy_start_date = fy_start_date

        self.formatted_start_date = start_date.strftime('%m/%d/%y')
        self.formatted_end_date = end_date.strftime('%m/%d/%y')

        self.fy_name = fy_name
        self.cadence_short_name = cadence_name
        self.cadence_long_name = cadence_name

        if self.cadence_short_name in list(calendar.month_name):
            self.cadence_short_name = datetime.strptime(self.cadence_short_name, "%B").strftime("%b")

        if report_type == "Executive Summary":
            print("Beginning Executive Summary")
            self.executiveReport()
        elif report_type == "Region":
            print(f"Beginning Region Report for {self.division_name}")
            self.regionReport(self.division_name)
        elif report_type == "Department":
            print(f"Beginning Department Report for {self.division_name}")
            self.departmentReport(self.division_name)
        else:
            print(f"Beginning Monthly Report for {self.division_name}")
            self.monthlyReport(self.division_name)
        
        global report_name
        report_name = fy_name + " " + cadence_name + " Report"
        Path(os.path.join(os.getcwd(),report_folder, fy_name, cadence_name)).mkdir(parents=True, exist_ok=True)
        
        modified_path = report_name + " - " + self.division_name + ".pdf"
        self.doc = ReportDocTemplate(os.path.join(os.getcwd(),report_folder, fy_name, cadence_name, modified_path), pagesize=LETTER, topMargin=inch/2, leftMargin=inch/2, rightMargin=inch/2, bottomMargin=inch/2)
        self.doc.multiBuild(self.elements, canvasmaker=ReportCanvas)

    def glossary(self):    
        print(f"Starting Glossary")

        self.elements.append(Paragraph("Glossary", rf.pageHeaderStyle))
        self.elements.append(Spacer(0, inch/4))


        for CategoryName, CategoryDict in self.glossary_dict.items():
            self.elements.append(Paragraph(CategoryName,rf.tableHeaderStyle))
            self.elements.append(Spacer(0, inch/10))

            glossary_data = []
            for Name, Definition in CategoryDict.items():
                glossary_data.append([Paragraph(Name,rf.GlossaryNameStyle), Paragraph(Definition,rf.GlossaryDefinitionStyle)])
        
            glossary_table = Table(glossary_data,colWidths=[3*inch,4.5*inch],style=rf.GlossaryTableStyle)
            self.elements.append(glossary_table)
            self.elements.append(Spacer(0, inch/10))
        
        self.elements.append(PageBreak())

    def tableOfContents(self):

        toc = TableOfContents()
        toc.levelStyles = [rf.TOCSectionStyle, rf.TOCSubSectionStyle]

        self.elements.append(Paragraph("Table of Contents", rf.TOCHeaderStyle))
        self.elements.append(Spacer(0, inch/4))

        self.elements.append(toc)
        
        self.elements.append(PageBreak())
                                                         
    def executiveReport(self):
        self.quarterlyTitlePage()

        self.tableOfContents()
        self.glossary()
        
        self.allAgency()
        #self.walkerGrid() 
        
    def testReport(self, department=None):
        self.quarterlyTitlePage()
        self.tableOfContents()
        self.glossary()

        self.indicators()

    def departmentReport(self, department):
        
        self.quarterlyTitlePage()
        
        self.tableOfContents()
        self.glossary()
        
        #self.allAgency()
        #self.walkerGrid() 
        
        
        for Region, DeptDict in self.master_dict.items():
            for Dept, ProgTypeDict in DeptDict.items():
                if Dept==department:
                    self.division(department=Dept) 
                    self.programPagesTitlePage(department)
                    for ProgType, ProgDict in ProgTypeDict.items():
                       for Prog, MergedIDDict in ProgDict.items():
                            self.programPage(ProgType, Prog, MergedIDDict)  
        
    def regionReport(self, region=None):
        self.quarterlyTitlePage()

        self.tableOfContents()
        self.glossary()
        
        self.allAgency()
        #self.walkerGrid() 
        
        for Region, DeptDict in self.master_dict.items():
            if region==Region:
                self.division(region=Region) 
                for Dept, ProgTypeDict in DeptDict.items():
                    self.division(department=Dept) 
                    self.programPagesTitlePage(Dept)
                    for ProgType, ProgDict in ProgTypeDict.items():
                       for Prog, MergedIDDict in ProgDict.items():
                            self.programPage(ProgType, Prog, MergedIDDict)   
        
    def monthlyReport(self, divison):
        self.monthlyTitlePage()
        
        self.tableOfContents()
        self.glossary()
        
        if divison == 'Los Angeles County':
            for Region, DeptDict in self.master_dict.items():
                if divison == Region:
                    self.division(region=Region) 
                    for Dept, ProgTypeDict in DeptDict.items():
                        self.division(department=Dept) 
                        self.programPagesTitlePage(Dept)
                        for ProgType, ProgDict in ProgTypeDict.items():
                            for Prog, MergedIDDict in ProgDict.items():
                                    self.programPage(ProgType, Prog, MergedIDDict)
        else:
            for Region, DeptDict in self.master_dict.items():
                for Dept, ProgTypeDict in DeptDict.items():
                    if Dept==divison:
                        #self.division(department=Dept) 
                        self.programPagesTitlePage(divison)
                        for ProgType, ProgDict in ProgTypeDict.items():
                            for Prog, MergedIDDict in ProgDict.items():
                                self.programPage(ProgType, Prog, MergedIDDict) 
        ## call upload function                               
    def quarterlyTitlePage(self):
        drawing = Drawing(width=inch*4.75, height=inch*7)
        rectangle = Rect(0, inch*3, inch, inch*7.75)
        rectangle.fillColor = rf.PATHBlue
        rectangle.strokeColor = rf.PATHBlue
        rectangle.strokeWidth = 0
        drawing.add(rectangle)
        
        rectangle = Rect(inch*1.25,inch,inch, inch*7.75)
        rectangle.fillColor = rf.PATHRed
        rectangle.strokeColor = rf.PATHRed
        rectangle.strokeWidth = 0
        drawing.add(rectangle)

        rectangle = Rect(inch*2.5,0,inch, inch*7.75)
        rectangle.fillColor = rf.PATHGreen
        rectangle.strokeColor = rf.PATHGreen
        rectangle.strokeWidth = 0
        drawing.add(rectangle)

        rectangle = Rect(inch*3.75,inch*1.5,inch, inch*7.75)
        rectangle.fillColor = rf.PATHPurple
        rectangle.strokeColor = rf.PATHPurple
        rectangle.strokeWidth = 0
        drawing.add(rectangle)

        drawing.hAlign = 'CENTER'

        titleLabel = Label()
        titleLabel.setText(self.fy_name)
        titleLabel.fontSize = 150
        titleLabel.fontName = 'MontserratSemiBold'
        titleLabel.dy = 6*inch
        titleLabel.dx = 1.25*inch
        drawing.add(titleLabel)
        
        titleLabel = Label()
        titleLabel.setText(self.cadence_short_name)
        
        titleLabel.fontSize = 150
        titleLabel.fontName = 'Montserrat'
        titleLabel.dy = 4.25*inch
        titleLabel.dx = 0
        drawing.add(titleLabel)

        self.elements.append(drawing)

        title = self.division_name
        if title == "Permanent Supportive Services (DTLA Place Based)":
            title = "PSS (DTLA Place Based)"
        elif title == "Permanent Supportive Services (Non DTLA Place Based)":
            title = "PSS (Non DTLA Place Based)"
        elif title == "Permanent Supportive Services (Scattered)":
            title = "PSS (Scattered)"
        elif title == "Interim Housing LA (Greater LA)":
            title = "Interim Housing (Greater LA)"
        elif title == "Interim Housing LA (SPA 4)":
            title = "Interim Housing (SPA 4)"

        titleLabelText = Paragraph(f"{title}",rf.titlePageStyle)
        self.elements.append(titleLabelText)   

        titleLabelText = Paragraph("Performance Report",rf.titlePageStyle)
        self.elements.append(titleLabelText)
        
        self.elements.append(Spacer(0,inch))

        titlePageTableData = []
        formattedtitleSubHeadingText = Paragraph(f"{self.formatted_start_date} - {self.formatted_end_date}",rf.titlePageSubHeaderStyle)
        formattedtitleSubHeadingText1 = Paragraph("Data & Evaluation Division",rf.titlePageSubHeaderStyle)
        formattedtitleSubHeadingText2 = Paragraph("Quality Assurance and Compliance Department",rf.titlePageSubHeaderStyle)
        formattedtitleSubHeadingText3 = Paragraph(f"Prepared {datetime.now().strftime('%B %Y')}",rf.titlePageSubHeaderStyle)
        titleSubHeadingTextData = [[formattedtitleSubHeadingText],[formattedtitleSubHeadingText1],[formattedtitleSubHeadingText2],[formattedtitleSubHeadingText3]]

        def get_image(path, width=4*inch):
            img = utils.ImageReader(path)
            iw, ih = img.getSize()
            aspect = ih / float(iw)
            return Image(path, width=width, height=(width * aspect))


        img = get_image(os.path.join(image_directory,'pathlogo.png'), width=1.5*inch)
        img.hAlign = 'RIGHT'
        img.vAlign = 'BOTTOM'
        
        titlePageTableData.append([titleSubHeadingTextData,img])

        
        titlePageTable = Table(titlePageTableData,style=rf.ProgTypeIndicatorsAlignmentTableStyle,colWidths=[6*inch,1.5*inch])
        self.elements.append(titlePageTable)

        self.elements.append(PageBreak())

    def monthlyTitlePage(self):
        drawing = Drawing(width=inch*4.75, height=inch*6)
        rectangle = Rect(0, inch*3, inch, inch*7.75)
        rectangle.fillColor = rf.PATHBlue
        rectangle.strokeColor = rf.PATHBlue
        rectangle.strokeWidth = 0
        drawing.add(rectangle)
        
        rectangle = Rect(inch*1.25,inch,inch, inch*7.75)
        rectangle.fillColor = rf.PATHRed
        rectangle.strokeColor = rf.PATHRed
        rectangle.strokeWidth = 0
        drawing.add(rectangle)

        rectangle = Rect(inch*2.5,0,inch, inch*7.75)
        rectangle.fillColor = rf.PATHGreen
        rectangle.strokeColor = rf.PATHGreen
        rectangle.strokeWidth = 0
        drawing.add(rectangle)

        rectangle = Rect(inch*3.75,inch*1.5,inch, inch*7.75)
        rectangle.fillColor = rf.PATHPurple
        rectangle.strokeColor = rf.PATHPurple
        rectangle.strokeWidth = 0
        drawing.add(rectangle)

        drawing.hAlign = 'CENTER'

        titleLabel = Label()
        titleLabel.setText(self.fy_name)
        titleLabel.fontSize = 150
        titleLabel.fontName = 'MontserratSemiBold'
        titleLabel.dy = 5*inch
        titleLabel.dx = 1.25*inch
        drawing.add(titleLabel)
        
        self.elements.append(drawing)

        title = self.division_name
        if title == "Permanent Supportive Services (DTLA Place Based)":
            title = "PSS (DTLA Place Based)"
        elif title == "Permanent Supportive Services (Non DTLA Place Based)":
            title = "PSS (Non DTLA Place Based)"
        elif title == "Permanent Supportive Services (Scattered)":
            title = "PSS (Scattered)"
        elif title == "Interim Housing LA (Greater LA)":
            title = "Interim Housing (Greater LA)"
        elif title == "Interim Housing LA (SPA 4)":
            title = "Interim Housing (SPA 4)"                        
            

        titleLabelText = Paragraph(f"{self.cadence_long_name}",rf.titlesubPageStyle)
        self.elements.append(titleLabelText)
        self.elements.append(Spacer(0,.8*inch))

        titleLabelText = Paragraph(f"{title}",rf.titlePageStyle)
        self.elements.append(titleLabelText)   

        titleLabelText = Paragraph("Monthly Report",rf.titlePageStyle)
        self.elements.append(titleLabelText)
        
        self.elements.append(Spacer(0,inch))

        titlePageTableData = []
        formattedtitleSubHeadingText = Paragraph(f"{self.formatted_start_date} - {self.formatted_end_date}",rf.titlePageSubHeaderStyle)
        formattedtitleSubHeadingText1 = Paragraph("Data & Evaluation Division",rf.titlePageSubHeaderStyle)
        formattedtitleSubHeadingText2 = Paragraph("Quality Assurance and Compliance Department",rf.titlePageSubHeaderStyle)
        formattedtitleSubHeadingText3 = Paragraph(f"Prepared {datetime.now().strftime('%B %Y')}",rf.titlePageSubHeaderStyle)
        titleSubHeadingTextData = [[formattedtitleSubHeadingText],[formattedtitleSubHeadingText1],[formattedtitleSubHeadingText2],[formattedtitleSubHeadingText3]]

        def get_image(path, width=4*inch):
            img = utils.ImageReader(path)
            iw, ih = img.getSize()
            aspect = ih / float(iw)
            return Image(path, width=width, height=(width * aspect))


        img = get_image(os.path.join(image_directory,'pathlogo.png'), width=1.5*inch)
        img.hAlign = 'RIGHT'
        img.vAlign = 'BOTTOM'
        
        titlePageTableData.append([titleSubHeadingTextData,img])

        
        titlePageTable = Table(titlePageTableData,style=rf.ProgTypeIndicatorsAlignmentTableStyle,colWidths=[6*inch,1.5*inch])
        self.elements.append(titlePageTable)

        self.elements.append(PageBreak())

    def programPagesTitlePage(self, department):
        drawing = Drawing(width=inch*4.75, height=inch*7)
        rectangle = Rect(0, inch*3, inch, inch*7.75)
        rectangle.fillColor = rf.PATHBlue
        rectangle.strokeColor = rf.PATHBlue
        rectangle.strokeWidth = 0
        drawing.add(rectangle)
        
        rectangle = Rect(inch*1.25,inch,inch, inch*7.75)
        rectangle.fillColor = rf.PATHRed
        rectangle.strokeColor = rf.PATHRed
        rectangle.strokeWidth = 0
        drawing.add(rectangle)

        rectangle = Rect(inch*2.5,0,inch, inch*7.75)
        rectangle.fillColor = rf.PATHGreen
        rectangle.strokeColor = rf.PATHGreen
        rectangle.strokeWidth = 0
        drawing.add(rectangle)

        rectangle = Rect(inch*3.75,inch*1.5,inch, inch*7.75)
        rectangle.fillColor = rf.PATHPurple
        rectangle.strokeColor = rf.PATHPurple
        rectangle.strokeWidth = 0
        drawing.add(rectangle)

        drawing.hAlign = 'CENTER'

        self.elements.append(drawing)

        titleLabelText = Paragraph("Program Summaries",rf.programsummarytitlePageStyle)
        self.elements.append(titleLabelText)   

        self.elements.append(Spacer(0,1*inch))

        titlePageTableData = []

        def get_image(path, width=4*inch):
            img = utils.ImageReader(path)
            iw, ih = img.getSize()
            aspect = ih / float(iw)
            return Image(path, width=width, height=(width * aspect))


        img = get_image(os.path.join(image_directory,'pathlogo.png'), width=1.5*inch)
        img.hAlign = 'RIGHT'
        img.vAlign = 'BOTTOM'
        
        titlePageTableData.append([[],img])
        
        titlePageTable = Table(titlePageTableData,style=rf.ProgTypeIndicatorsAlignmentTableStyle,colWidths=[6*inch,1.5*inch])
        self.elements.append(titlePageTable)

        self.elements.append(PageBreak())

    def allAgency(self):
        # Agency Indicators by Department
        #self.elements.append(Paragraph("Agency Indicators by Department", rf.pageHeaderStyle))
        #self.elements.append(Spacer(0, inch/4))
        
        #print(f"Starting Agency Indicators by Department")
        #self.agencyIndicatorsByDept()
        
        #self.elements.append(PageBreak())

        # Program Type Indicators
        self.elements.append(Paragraph("Program Type Indicators, All Agency", rf.pageHeaderStyle))
        self.elements.append(Spacer(0, inch/4))

        print("Starting Agency Program Type Indicators")     
        self.indicators()

        self.elements.append(PageBreak())

        # Agency Demographics
        self.elements.append(Paragraph("All Agency Demographics", rf.pageHeaderStyle))   
        self.elements.append(Spacer(0, inch/10))
        self.elements.append(Paragraph("Fiscal Year-to-Date", rf.subSectionHeaderStyle))
        self.elements.append(Spacer(0, inch/4))

        print("Starting Agency Demographics")     
        self.demographics()

        self.elements.append(PageBreak())

    def agencyIndicatorsByDept(self):    
        for ProgramType, IndicatorCategoryDict in self.agency_indicator_functions_dict.items():
            if ProgramType == "All Agency":                
                for IndicatorCategoryName, IndicatorNameDict in IndicatorCategoryDict.items(): 
                    self.elements.append(Paragraph(IndicatorCategoryName, rf.sectionHeaderStyle))
                    #self.elements.append(Spacer(10, inch/10))

                    table_headers = [""]
                    temp_total_results = []
                    dept_data = []
                    region_data = []
                    all_agency_data = []


                    # Non-LA regions and departments 
                    for region, departments in self.master_dict.items():
                        if region == 'Los Angeles County':                   
                            for department in departments:
                                temp_dept_data = []
                                for IndicatorName, IndicatorDetailsDict in IndicatorNameDict.items():
                                    if IndicatorDetailsDict['IndicatorFunction']: 
                                        parameters = IndicatorDetailsDict['IndicatorParameter']
                                        parameters.update({"start_date": self.start_date, "end_date": self.end_date})
                                        result = IndicatorDetailsDict['IndicatorFunction'](**parameters, department=[department])
                                        format_value = IndicatorDetailsDict['Format']
                                        if result: 
                                            formatted_result = format_value.format(result)
                                        else:
                                            formatted_result = "-"
                                        temp_dept_data.append(formatted_result)
                                temp_dept_data.insert(0, department)
                                dept_data.append(temp_dept_data)
                        else:
                            temp_region_data = []
                            for IndicatorName, IndicatorDetailsDict in IndicatorNameDict.items():
                                if IndicatorDetailsDict['IndicatorFunction']: 
                                    parameters = IndicatorDetailsDict['IndicatorParameter']
                                    parameters.update({"start_date": self.start_date, "end_date": self.end_date})
                                    result = IndicatorDetailsDict['IndicatorFunction'](**parameters, region=[region])
                                    format_value = IndicatorDetailsDict['Format']
                                    if result: 
                                        formatted_result = format_value.format(result)
                                    else:
                                        formatted_result = "-"

                                    temp_region_data.append(formatted_result)
                            temp_region_data.insert(0, region)
                            region_data.append(temp_region_data)

                    # LA 
                    temp_la_results = []
                    for IndicatorName, IndicatorDetailsDict in IndicatorNameDict.items():
                        if IndicatorDetailsDict['IndicatorFunction']: 
                            parameters = IndicatorDetailsDict['IndicatorParameter']
                            parameters.update({"start_date": self.start_date, "end_date": self.end_date})
                            result = IndicatorDetailsDict['IndicatorFunction'](**parameters, region=['Los Angeles County'])
                            format_value = IndicatorDetailsDict['Format']
                            if result: 
                                formatted_result = format_value.format(result)
                            else:
                                formatted_result = "-"

                            temp_la_results.append(formatted_result)
                    temp_la_results.insert(0, 'Los Angeles County') 
                    region_data.append(temp_la_results)   

                    for IndicatorName, IndicatorDetailsDict in IndicatorNameDict.items():
                        if IndicatorDetailsDict['IndicatorFunction']:
                            table_headers.append(IndicatorName)
                            parameters = IndicatorDetailsDict['IndicatorParameter']
                            parameters.update({"start_date": self.start_date, "end_date": self.end_date})
                            result = IndicatorDetailsDict['IndicatorFunction'](**parameters)
                            format_value = IndicatorDetailsDict['Format']
                            if result: 
                                formatted_result = format_value.format(result)
                            else:
                                formatted_result = "-"

                            temp_total_results.append(formatted_result)

                        else:
                            temp_total_results.append(IndicatorName)

                    # Agency-wide data
                    temp_all_agency_results = []
                    for IndicatorName, IndicatorDetailsDict in IndicatorNameDict.items():
                        if IndicatorDetailsDict['IndicatorFunction']: 
                            parameters = IndicatorDetailsDict['IndicatorParameter']
                            parameters.update({"start_date": self.start_date, "end_date": self.end_date})
                            result = IndicatorDetailsDict['IndicatorFunction'](**parameters)
                            format_value = IndicatorDetailsDict['Format']
                            if result: 
                                formatted_result = format_value.format(result)
                            else:
                                formatted_result = "-"

                            temp_all_agency_results.append(formatted_result)
                    temp_all_agency_results.insert(0, f'{self.cadence_short_name}\nTotal')
                    all_agency_data.append(temp_all_agency_results)

                    temp_ytd_agency_results = []
                    for IndicatorName, IndicatorDetailsDict in IndicatorNameDict.items():
                        if IndicatorDetailsDict['IndicatorFunction']: 
                            parameters = IndicatorDetailsDict['IndicatorParameter']
                            parameters.update({"start_date": self.fy_start_date, "end_date": self.end_date})
                            result = IndicatorDetailsDict['IndicatorFunction'](**parameters)
                            format_value = IndicatorDetailsDict['Format']
                            if result: 
                                formatted_result = format_value.format(result)
                            else:
                                formatted_result = "-"

                            temp_ytd_agency_results.append(formatted_result)
                    temp_ytd_agency_results.insert(0, 'Agency\nYTD')
                    all_agency_data.append(temp_ytd_agency_results)        

                    table_headers = [Paragraph(cell, rf.IndicatorByDeptIndicatorNameStyle) for cell in table_headers] # set paragraph style for header cells     
                    region_data.insert(0, table_headers)


                    # transpose
                    region_data = list(map(list, zip(*region_data)))
                    dept_data = list(map(list, zip(*dept_data)))
                    all_agency_data = list(map(list, zip(*all_agency_data)))
                
                    region_table_data = m.shorten_and_format(region_data)
                    dept_table_data = m.shorten_and_format(dept_data)
                    all_agency_table_data = m.shorten_and_format(all_agency_data)
                    

                    region_table = Table(region_table_data,
                                        colWidths=rf.agencyIndicatorsByDeptRegionColWidths,
                                            rowHeights=rf.agencyIndicatorsByDeptAllRowHeights)     
                    region_table.setStyle(rf.agencyIndicatorsByDeptRegionTableStyle)

                    dept_table = Table(dept_table_data, 
                                    colWidths=rf.agencyIndicatorsByDeptDeptColWidths, 
                                    rowHeights=rf.agencyIndicatorsByDeptAllRowHeights)     
                    dept_table.setStyle(rf.agencyIndicatorsByDeptDeptTableStyle)

                    all_agency_table = Table(all_agency_table_data, 
                                    colWidths=[.55*inch,.55*inch], 
                                    rowHeights=rf.agencyIndicatorsByDeptAllRowHeights)     
                    all_agency_table.setStyle(rf.agencyIndicatorsAllAgencyTableStyle)

                    spacer_table_data = [""]
                    spacer_table = Table(spacer_table_data, colWidths=inch*.1, rowHeights=rf.agencyIndicatorsByDeptAllRowHeights)
                    container_data = [[region_table, spacer_table, dept_table, spacer_table, all_agency_table]]
                    if IndicatorCategoryName == 'Days to First Service':
                        container_data.append([Paragraph("Note: Includes only service data from HMIS. Case notes, assessments, or any other additional information are not included.", rf.tableFooterStyle)])
                    
                    if IndicatorCategoryName == 'Data Quality':
                        container_data.append([Paragraph("Note: Participant data from Santa Barbara is incomplete, which may impact the overall score.", rf.tableFooterStyle)])

                    container_table = Table(container_data)
                    container_table.setStyle(rf.agencyIndicatorsByDeptContainerTableStyle)
                    self.elements.append(container_table)

                    self.elements.append(Spacer(10, inch/6))
                    print("Loaded", IndicatorCategoryName, "Table from Agency Indicators by Department")

                print("Loaded", ProgramType, "Table from Agency Indicators by Department")

    def indicators(self, program_id=None, region=None, department=None):        
        program_type_list = []
        format = []
        cadence_parameters = ({"start_date": self.start_date, "end_date": self.end_date})
        ytd_parameters = ({"start_date": self.fy_start_date, "end_date": self.end_date})

        if program_id:
            cadence_parameters.update({"program_id":[program_id]})
            ytd_parameters.update({"program_id":[program_id]})
            program_type_list = self.returnProgTypes(program_id=program_id)
        elif region:
            cadence_parameters.update({"region":[region]})
            ytd_parameters.update({"region":[region]})
            program_type_list = self.returnProgTypes(region=region)
        elif department:
            cadence_parameters.update({"department":[department]})
            ytd_parameters.update({"department":[department]})
            program_type_list = self.returnProgTypes(department=department)
        else:
            program_type_list = self.returnProgTypes()
            format = 'All Agency'

        cadence_reference_parameters = cadence_parameters.copy()
        ytd_reference_parameters = ytd_parameters.copy()
        
        print(f"Starting Agency Indicators for {program_id, region, department}")

        self.agencyIndicators(cadence_reference_parameters, ytd_reference_parameters)
        

        if "Outreach Services" in program_type_list:           
            cadence_parameters = cadence_reference_parameters.copy()
            cadence_parameters.update({"program_type":["Outreach Services"]})
            ytd_parameters = ytd_reference_parameters.copy()
            ytd_parameters.update({"program_type":["Outreach Services"]})
            
            print(f"Starting {cadence_parameters['program_type'][0]} for {program_id, region, department}")

            self.elements.append(Paragraph(f"{cadence_parameters['program_type'][0]} Indicators", rf.subSectionHeaderStyle))
            self.elements.append(Spacer(10, inch/6))
            self.outreachIndicators(cadence_parameters, ytd_parameters)
            if len(program_type_list) > 1:
                self.elements.append(Spacer(10, inch/10))
        
        if "Interim Housing Services" in program_type_list:
            cadence_parameters = cadence_reference_parameters.copy()
            cadence_parameters.update({"program_type":["Interim Housing Services"]})
            ytd_parameters = ytd_reference_parameters.copy()
            ytd_parameters.update({"program_type":["Interim Housing Services"]})
            print(f"Starting {cadence_parameters['program_type'][0]} for {program_id, region, department}")

            self.elements.append(Paragraph(f"{cadence_parameters['program_type'][0]} Indicators", rf.subSectionHeaderStyle))
            self.elements.append(Spacer(10, inch/6))
            self.interimIndicators(cadence_parameters, ytd_parameters)
            if len(program_type_list) > 1:
                self.elements.append(Spacer(10, inch/10))
       
        if "Housing Navigation Services" in program_type_list:
            cadence_parameters = cadence_reference_parameters.copy()
            cadence_parameters.update({"program_type":["Housing Navigation Services"]})
            ytd_parameters = ytd_reference_parameters.copy()
            ytd_parameters.update({"program_type":["Housing Navigation Services"]})
            
            print(f"Starting {cadence_parameters['program_type'][0]} for {program_id, region, department}")

            self.elements.append(Paragraph(f"{cadence_parameters['program_type'][0]} Indicators", rf.subSectionHeaderStyle))
            self.elements.append(Spacer(10, inch/6))
            
            self.housingNavIndicators(cadence_parameters, ytd_parameters)
            if len(program_type_list) > 1:
                self.elements.append(Spacer(10, inch/10))
        
        if "Rapid Rehousing Services" in program_type_list:
            cadence_parameters = cadence_reference_parameters.copy()
            cadence_parameters.update({"program_type":["Rapid Rehousing Services"]})
            ytd_parameters = ytd_reference_parameters.copy()
            ytd_parameters.update({"program_type":["Rapid Rehousing Services"]})
            
            print(f"Starting {cadence_parameters['program_type'][0]} for {program_id, region, department}")

            self.elements.append(Paragraph(f"{cadence_parameters['program_type'][0]} Indicators", rf.subSectionHeaderStyle))
            self.elements.append(Spacer(10, inch/6))
            
            self.rapidIndicators(cadence_parameters, ytd_parameters)
            if len(program_type_list) > 1: 
                self.elements.append(Spacer(10, inch/10))
                if format == 'All Agency':
                    self.elements.append(PageBreak())
                    ##### PAGE BREAK FOR FORMATTING 
        if "Site Based Housing & Services" in program_type_list:
            cadence_parameters = cadence_reference_parameters.copy()
            cadence_parameters.update({"program_type":["Site Based Housing & Services"]})
            ytd_parameters = ytd_reference_parameters.copy()
            ytd_parameters.update({"program_type":["Site Based Housing & Services"]})
            
            print(f"Starting {cadence_parameters['program_type'][0]} for {program_id, region, department}")

            self.elements.append(Paragraph(f"{cadence_parameters['program_type'][0]} Indicators", rf.subSectionHeaderStyle))
            self.elements.append(Spacer(10, inch/6))
            
            self.siteBasedIndicators(cadence_parameters, ytd_parameters)
            if len(program_type_list) > 1:
                self.elements.append(Spacer(10, inch/10))
        if "Scattered Site Housing & Services" in program_type_list:
            cadence_parameters = cadence_reference_parameters.copy()
            cadence_parameters.update({"program_type":["Scattered Site Housing & Services"]})
            ytd_parameters = ytd_reference_parameters.copy()
            ytd_parameters.update({"program_type":["Scattered Site Housing & Services"]})
            
            print(f"Starting {cadence_parameters['program_type'][0]} for {program_id, region, department}")

            self.elements.append(Paragraph(f"{cadence_parameters['program_type'][0]} Indicators", rf.subSectionHeaderStyle))
            self.elements.append(Spacer(10, inch/6))
            
            self.scatteredSiteIndicators(cadence_parameters, ytd_parameters)
            if len(program_type_list) > 1:
                self.elements.append(Spacer(10, inch/10))
        
        if "Prevention & Diversion Services" in program_type_list:
            cadence_parameters = cadence_reference_parameters.copy()
            cadence_parameters.update({"program_type":["Prevention & Diversion Services"]})
            ytd_parameters = ytd_reference_parameters.copy()
            ytd_parameters.update({"program_type":["Prevention & Diversion Services"]})
            
            print(f"Starting {cadence_parameters['program_type'][0]} for {program_id, region, department}")

            self.elements.append(Paragraph(f"{cadence_parameters['program_type'][0]} Indicators", rf.subSectionHeaderStyle))
            self.elements.append(Spacer(10, inch/6))
            
            self.preventionIndicators(cadence_parameters, ytd_parameters)
            if len(program_type_list) > 1:
                self.elements.append(Spacer(10, inch/10))
        
        if "Care Coordination" in program_type_list:
            cadence_parameters = cadence_reference_parameters.copy()
            cadence_parameters.update({"program_type":["Care Coordination"]})
            ytd_parameters = ytd_reference_parameters.copy()
            ytd_parameters.update({"program_type":["Care Coordination"]})
            
            print(f"Starting {cadence_parameters['program_type'][0]} for {program_id, region, department}")

            self.elements.append(Paragraph(f"{cadence_parameters['program_type'][0]} Indicators", rf.subSectionHeaderStyle))
            self.elements.append(Spacer(10, inch/6))
            
            self.careCoordIndicators(cadence_parameters, ytd_parameters)
            if len(program_type_list) > 1:
                self.elements.append(Spacer(10, inch/10)) 

        if "Employment Services" in program_type_list:
            cadence_parameters = cadence_reference_parameters.copy()
            cadence_parameters.update({"program_type":["Employment Services"]})
            ytd_parameters = ytd_reference_parameters.copy()
            ytd_parameters.update({"program_type":["Employment Services"]})
            
            print(f"Starting {cadence_parameters['program_type'][0]} for {program_id, region, department}")

            self.elements.append(Paragraph(f"{cadence_parameters['program_type'][0]} Indicators", rf.subSectionHeaderStyle))
            self.elements.append(Spacer(10, inch/6))
            
            self.employmentIndicators(cadence_parameters, ytd_parameters)
            if len(program_type_list) > 1:
                self.elements.append(Spacer(10, inch/10))

        if "Housing & Landlord Partnerships" in program_type_list:
            cadence_parameters = cadence_reference_parameters.copy()
            cadence_parameters.update({"program_type":["Housing & Landlord Partnerships"]})
            ytd_parameters = ytd_reference_parameters.copy()
            ytd_parameters.update({"program_type":["Housing & Landlord Partnerships"]})
            
            print(f"Starting {cadence_parameters['program_type'][0]} for {program_id, region, department}")

            self.elements.append(Paragraph(f"{cadence_parameters['program_type'][0]} Indicators", rf.subSectionHeaderStyle))
            self.elements.append(Spacer(10, inch/6))
            
            self.housingPartnershipsIndicators(cadence_parameters, ytd_parameters)
            if len(program_type_list) > 1:
                self.elements.append(Spacer(10, inch/10))

        if "Behavioral Health Services" in program_type_list:
            cadence_parameters = cadence_reference_parameters.copy()
            cadence_parameters.update({"program_type":["Behavioral Health Services"]})
            ytd_parameters = ytd_reference_parameters.copy()
            ytd_parameters.update({"program_type":["Behavioral Health Services"]})
            
            print(f"Starting {cadence_parameters['program_type'][0]} for {program_id, region, department}")

            self.elements.append(Paragraph(f"{cadence_parameters['program_type'][0]} Indicators", rf.subSectionHeaderStyle))
            self.elements.append(Spacer(10, inch/6))
            
            self.behavioralHealthIndicators(cadence_parameters, ytd_parameters)
            if len(program_type_list) > 1:
                self.elements.append(Spacer(10, inch/10))

        if "Access Center Services" in program_type_list:
            cadence_parameters = cadence_reference_parameters.copy()
            cadence_parameters.update({"program_type":["Access Center Services"]})
            ytd_parameters = ytd_reference_parameters.copy()
            ytd_parameters.update({"program_type":["Access Center Services"]})
            
            print(f"Starting {cadence_parameters['program_type'][0]} for {program_id, region, department}")

            self.elements.append(Paragraph(f"{cadence_parameters['program_type'][0]} Indicators", rf.subSectionHeaderStyle))
            self.elements.append(Spacer(10, inch/6))
            
            self.accessCenterIndicators(cadence_parameters, ytd_parameters)             
            if len(program_type_list) > 1:
                self.elements.append(Spacer(10, inch/10))       
        
    def demographics(self, region=None, department=None):
        
        parameters = ({"start_date": self.fy_start_date, "end_date": self.end_date})
        
        if region:
            title = region
            parameters.update({"region":[region]})
        elif department:
            title = department
            parameters.update({"department":[department]})
        else:
            title = "All Agency"

        total_count = m.active_clients(**parameters)

        def get_image(path, width=4*inch):
            img = utils.ImageReader(path)
            iw, ih = img.getSize()
            aspect = ih / float(iw)
            return Image(path, width=width, height=(width * aspect))

        def get_image_height(path, height=4*inch):
            img = utils.ImageReader(path)
            iw, ih = img.getSize()
            aspect = iw / float(ih)
            return Image(path, width=(height * aspect), height=height)

        
        # Chart visual specifications
        bar_width = 0.8
        width = 3.625
        height = 2.167
        image_dpi = 600
        inches_to_trim = 0.1

        stem_color = '#004C90'
        marker_size = 3

        # Font Specifications
        font_size = 7
        data_label_font_size = 6
        font_name = 'Open Sans'
        font_color = 'black'

        _, ages = m.age_bins_5y(**parameters)
        age_categories = age_counts = list(ages.keys()) 
        age_counts = list(ages.values())
        percentages = [(cat_count / total_count)*100 for cat_count in age_counts]

        fig, ax = plt.subplots(figsize=(width, height), dpi=image_dpi)
        ax = plt.stem(age_categories,percentages, linefmt=stem_color, markerfmt=stem_color, basefmt='none',orientation='horizontal')
        plt.setp(ax.markerline, markersize=marker_size) 
        ax = plt.gca()

        ax.set_xlim(0)

        ax.xaxis.set_major_formatter(PercentFormatter(1.0))
        labels = ['{:.0f}%'.format(x) for x in ax.get_xticks()]
        ax.set_xticklabels(labels, fontsize=font_size, color=font_color, fontname=font_name) 

        y_ticks = list(range(len(ages)))
        ax.set_yticks(y_ticks)
        ax.set_yticklabels(ages, fontsize=font_size, color=font_color, fontname=font_name)

        for y, percent in enumerate(percentages):
            ax.annotate('{:.1f}%'.format(percent), xy=(percent, y), xytext=(13, -2.4), textcoords='offset points', ha='center',
                            fontsize=data_label_font_size, color=font_color,fontname=font_name, weight='bold')

        ax.tick_params(axis='y', length=0)

        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        ax.xaxis.grid(alpha = 0.3)
        ax.set_axisbelow(True)

        filename = 'ageplot.png'
        plt.savefig(os.path.join(chart_directory, title, filename), transparent=True, bbox_inches='tight')

        cropped_image = IMG.open(os.path.join(chart_directory, title, filename))
        pixels_per_inch = cropped_image.info['dpi'][0]
        pixels_to_trim = int(inches_to_trim * pixels_per_inch)
        crop_box = (pixels_to_trim, pixels_to_trim, cropped_image.width - pixels_to_trim, cropped_image.height - pixels_to_trim)
        cropped_image = cropped_image.crop(crop_box)
        cropped_image.save(os.path.join(chart_directory, title, filename))

        age_plot = get_image(os.path.join(chart_directory, title, filename), width=3.75*inch)

        plt.close()

        # Age Table
        children_count = m.number_of_children(**parameters)
        transitional_youth = m.transitional_aged_youth(**parameters)
        senior_count = m.senior_count(**parameters)
        adult  = m.adult_count(**parameters)
        unknown = ages['Unknown'] 


        formatted_children = "{:,}".format(children_count)
        formatted_transitional_youth = "{:,}".format(transitional_youth)
        formatted_senior = "{:,}".format(senior_count)
        formatted_adult = "{:,}".format(adult)
        formatted_unknown = "{:,}".format(unknown)

        formatted_children_percentage = "{:.1%}".format(children_count/total_count)
        formatted_transitional_youth_percentage = "{:.1%}".format(transitional_youth/total_count)
        formatted_senior_percentage = "{:.1%}".format(adult/total_count)
        formatted_adult_percentage = "{:.1%}".format(senior_count/total_count)
        formatted_adult_unknown = "{:.1%}".format(unknown/total_count)

        header_text = ["", "#", "%"]
        formatted_header_text = [Paragraph(cell, rf.tableSecondaryHeader) for cell in header_text]

        age_data = [formatted_header_text]

        age_data.append([Paragraph("Children (<18)", rf.tableTextStyle), Paragraph(formatted_children, rf.tableValuesStyle), Paragraph(formatted_children_percentage, rf.tableTargetStyle)])
        age_data.append([Paragraph("Transitional Aged Youth (18-25)", rf.tableTextStyle), Paragraph(formatted_transitional_youth, rf.tableValuesStyle), Paragraph(formatted_transitional_youth_percentage, rf.tableTargetStyle)])
        age_data.append([Paragraph("Adults (25-65)", rf.tableTextStyle), Paragraph(formatted_adult, rf.tableValuesStyle), Paragraph(formatted_senior_percentage, rf.tableTargetStyle)])
        age_data.append([Paragraph("Seniors (65+)", rf.tableTextStyle), Paragraph(formatted_senior, rf.tableValuesStyle), Paragraph(formatted_adult_percentage, rf.tableTargetStyle)])
        age_data.append([Paragraph("Unknown", rf.tableTextStyle), Paragraph(formatted_unknown, rf.tableValuesStyle), Paragraph(formatted_adult_unknown, rf.tableTargetStyle)])

        age_table = Table(age_data, colWidths=[2.65*inch,.6*inch,.5*inch], rowHeights=[inch/5]*len(age_data),style=rf.demosTableStyle)
        row1demotable = Table([[age_table, age_plot]],style=rf.demoPageTableStyle)



        ### Race/Ethnicity

        # Chart visual specifications
        colors = ['#BFEBFB', '#73D2F6', '#26BAF1', '#0094CB', '#00688F',
                   '#0080b1','#00415c','#001c2a', '#d9d9d9']
        bar_width = 0.8
        width = 3.625
        height = 2.167
        image_dpi = 600
        inches_to_trim = 0.1

        # Font Specifications
        font_size = 7
        cat_font_size = 5
        data_label_font_size = 6
        font_name = 'Open Sans'
        font_color = 'black'

        race_full, races = m.race_ethnicity(**parameters)
        race_categories = list(races.keys()) 
        races_counts = list(races.values())
        percentages = [(cat_count / total_count)*100 for cat_count in races_counts]

        fig, ax = plt.subplots(figsize=(width, height), dpi=image_dpi)

        bars = ax.bar(race_categories, percentages, color=colors, width=bar_width)

        for bar, percentage in zip(bars, percentages):
            label = f'{percentage:.1f}%'
            ax.annotate(label, (bar.get_x() + bar.get_width() / 2, bar.get_height() + 1),
                        ha='center', va='bottom', fontsize=data_label_font_size, color=font_color, weight='bold')


        ax.set_xticks(range(len(race_categories))
        )
        ax.set_yticklabels([])  

        x_labels = [textwrap.fill(label, width=30, break_long_words=False, replace_whitespace=False,
                                  expand_tabs=False, initial_indent='', subsequent_indent='    ',
                                    drop_whitespace=True) for label in race_categories]
        ax.set_xticklabels(x_labels, fontsize=cat_font_size, color=font_color, fontname=font_name, rotation=45, ha='right')  

        ax.yaxis.set_major_formatter(PercentFormatter(1.0))
        labels = ['{:.0f}%'.format(x) for x in ax.get_yticks()]
        ax.set_yticklabels(labels, fontsize=font_size, color=font_color, fontname=font_name)


        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.yaxis.grid(alpha=0.3)
        ax.set_axisbelow(True)

        filename = 'raceplot.png'
        plt.savefig(os.path.join(chart_directory, title, filename), transparent=True, bbox_inches='tight')

        cropped_image = IMG.open(os.path.join(chart_directory, title, filename))
        pixels_per_inch = cropped_image.info['dpi'][0]
        pixels_to_trim = int(inches_to_trim * pixels_per_inch)
        crop_box = (pixels_to_trim, pixels_to_trim, cropped_image.width - pixels_to_trim, cropped_image.height - pixels_to_trim)
        cropped_image = cropped_image.crop(crop_box)
        cropped_image.save(os.path.join(chart_directory, title, filename))

        plt.close()

        race_plot = get_image(os.path.join(chart_directory, title, filename),
                          width=3.75*inch)
        


        header_text = ["", "#", "%"]
        formatted_header_text = [Paragraph(cell, rf.tableSecondaryHeader) for cell in header_text]
        race_table_data = [formatted_header_text]
        for race_cat, count in race_full.items():
            percentage = "{:.1%}".format(count/total_count)
            count = "{:,}".format(count)

            race_table_data.append([Paragraph(race_cat, rf.tableTextStyle),
                                     Paragraph(count, rf.tableValuesStyle),
                                      Paragraph(percentage, rf.tableTargetStyle)])


        race_table = Table(race_table_data,style=rf.demosTableStyle, colWidths=[2.65*inch,.6*inch,.5*inch], rowHeights=[inch/5]*len(race_table_data))

        row2demotable = Table([[race_plot,race_table]],style=rf.demoPageTableStyle)


        # Gender

        # Chart visual specifications
        colors = ['#d9d9d9','#b3e0fa', '#5cbff3', '#00aeef',
                   '#0080b1', '#001c2a', '#d9f0fd', '#8ad0f7']
        bar_width = 0.8
        width = 3.625
        height = 2.167
        image_dpi = 600
        inches_to_trim = 0.1

        # Font Specifications
        font_size = 7
        data_label_font_size = 6
        font_name = 'Open Sans'
        font_color = 'black'

        gender_full, gender  = m.gender_count(**parameters)
        gender_categories = list(gender.keys()) 
        gender_counts = list(gender.values())
        percentages = [(cat_count / m.active_clients(**parameters))*100 for cat_count in gender_counts]
        gender_categories.reverse()
        percentages.reverse()


        fig, ax = plt.subplots(figsize=(width, height), dpi=image_dpi)

        bars = ax.barh(gender_categories, percentages, color=colors, height=bar_width)

        for bar, percentage in zip(bars, percentages):
            label = f'{percentage:.1f}%'
            ax.annotate(label, (bar.get_width()+.2, bar.get_y() + bar.get_height() / 2),
                        ha='left', va='center', fontsize=data_label_font_size, color=font_color, weight='bold')


        ax.set_yticks(range(len(gender_categories)))
        ax.set_yticklabels(gender_categories, fontsize=font_size, color=font_color, fontname=font_name)

        ax.xaxis.set_major_formatter(PercentFormatter(1.0))
        labels = ['{:.0f}%'.format(x) for x in ax.get_xticks()]
        ax.set_xticklabels(labels, fontsize=font_size, color=font_color, fontname=font_name) 

        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.xaxis.grid(alpha = 0.3)
        ax.set_axisbelow(True)

        filename = 'genderplot.png'
        plt.savefig(os.path.join(chart_directory, title, filename), transparent=True, bbox_inches='tight')

        cropped_image = IMG.open(os.path.join(chart_directory, title, filename))
        pixels_per_inch = cropped_image.info['dpi'][0]
        pixels_to_trim = int(inches_to_trim * pixels_per_inch)
        crop_box = (pixels_to_trim, pixels_to_trim, cropped_image.width - pixels_to_trim, cropped_image.height - pixels_to_trim)
        cropped_image = cropped_image.crop(crop_box)
        cropped_image.save(os.path.join(chart_directory, title, filename))

        plt.close()
       
        gender_plot = get_image(os.path.join(chart_directory, title, filename),
                          width=3.75*inch)


        # gender table

        gender_categories = list(gender_full.keys()) 
        gender_counts = list(gender_full.values())
        percentages = [(cat_count / total_count)*100 for cat_count in gender_counts]

        header_text = ["", "#", "%"]
        formatted_header_text = [Paragraph(cell, rf.tableSecondaryHeader) for cell in header_text]
        gender_data = [formatted_header_text]
        for gender_cat, count in gender_full.items():
            percentage = "{:.1%}".format(count/total_count)
            count = "{:,}".format(count)

            gender_data.append([Paragraph(gender_cat, rf.tableTextStyle),
                                     Paragraph(count, rf.tableValuesStyle),
                                      Paragraph(percentage, rf.tableTargetStyle)])


        gender_table = Table(gender_data,style=rf.demosTableStyle, colWidths=[2.65*inch,.6*inch,.5*inch], rowHeights=[inch/5]*len(gender_data))

        row3demotable = Table([[gender_table, gender_plot]],style=rf.demoPageTableStyle)

        disability = m.disability_count(**parameters)

        disability_counts = list(disability.values())
        percentages = [(cat_count / total_count) for cat_count in disability_counts]

        header_text = ["", "#", "%"]
        formatted_header_text = [Paragraph(cell, rf.tableSecondaryHeader) for cell in header_text]
        disability_data = [formatted_header_text]
        for disability_cat, count in disability.items():
            percentage = "{:.1%}".format(count / total_count)
            count = "{:,}".format(count)



            if "Severe" in disability_cat:
                disability_cat = Paragraph(disability_cat, rf.DisabilitySevereTextStyle)
            else:
                disability_cat = Paragraph(disability_cat, rf.DisabilityTextStyle)
                

            disability_data.append([disability_cat,
                                    Paragraph(count, rf.tableValuesStyle),
                                    Paragraph(percentage, rf.tableTargetStyle)])
            
        footer_text = "Serere conditions are those that are expected to be long-continuing or of indefinite duration, substantially impedes the individual's ability to live independently, and could be improved by the provision of more suitable housing conditions."
        disability_data.append([Paragraph(footer_text,rf.tableFooterStyle)])

        disability_table = Table(disability_data, style=rf.DisabilityTableStyle, colWidths=[3.9*inch,.6*inch,.5*inch], rowHeights=[inch / 5] * len(disability_data))


        
        """

        # Vets

        bar_width = 0.8
        width = 3.625
        height = 2.167
        image_dpi = 600
        inches_to_trim = 0.1

        # Font Specifications
        font_size = 7
        data_label_font_size = 6
        font_name = 'Open Sans'
        font_color = 'black'


        colors = ['#00AEEF', '#B2E7FA', '#d9d9d9']
        veteran_status=m.veteran_status(**parameters)

        labels = ['Veteran', 'Non-Veteran','Unknown']
        sizes = [veteran_status['Veteran'], veteran_status['Not a Veteran'],
                 (veteran_status['Client Does Not Know']+veteran_status['Client Refused']+veteran_status['Data Not Collected'])]
        explode = (.1, 0, 0) 


        label_font = {'fontname':font_name, 'color':font_color, 'fontsize':font_size}
        fig, ax = plt.subplots(figsize=(width, height), dpi=image_dpi)

        patches, texts, pcts = ax.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%',wedgeprops={'linewidth': 1.0, 'edgecolor': 'white'},
            colors=colors, startangle=140, textprops=label_font)

        plt.setp(pcts, color=font_color, fontweight='bold',fontsize=data_label_font_size)


        filename = 'vetplot.png'
        plt.savefig(os.path.join(chart_directory, title, filename), transparent=True, bbox_inches='tight')

        cropped_image = IMG.open(os.path.join(chart_directory, title, filename))
        pixels_per_inch = cropped_image.info['dpi'][0]
        pixels_to_trim = int(inches_to_trim * pixels_per_inch)
        crop_box = (pixels_to_trim, pixels_to_trim, cropped_image.width - pixels_to_trim, cropped_image.height - pixels_to_trim)
        cropped_image = cropped_image.crop(crop_box)
        cropped_image.save(os.path.join(chart_directory, title, filename))

        plt.close()


        vet_plot = get_image(os.path.join(chart_directory, title, filename),
                          width=3.75*inch)
        


        header_text = ["", "#", "%"]
        formatted_header_text = [Paragraph(cell, rf.tableSecondaryHeader) for cell in header_text]
        vet_table_data = [formatted_header_text]
        for vet_cat, count in veteran_status.items():
            percentage = "{:.1%}".format(count/total_count)
            count = "{:,}".format(count)

            vet_table_data.append([Paragraph(vet_cat, rf.tableTextStyle),
                                     Paragraph(count, rf.tableValuesStyle),
                                      Paragraph(percentage, rf.tableTargetStyle)])


        vet_table = Table(vet_table_data,style=rf.demosTableStyle, colWidths=[2.75*inch,.5*inch,.5*inch], rowHeights=[inch/5]*len(vet_table_data))

        # Chronically Homeless

        bar_width = 0.8
        width = 3.625
        height = 2.167
        image_dpi = 600
        inches_to_trim = 0.1

        # Font Specifications
        font_size = 7
        data_label_font_size = 6
        font_name = 'Open Sans'
        font_color = 'black'


        colors = ['#0DB14B', '#BCE8C9']
        chronically_homeless = m.chronically_homeless_count(**parameters)

        labels = ['Chronically\nHomeless', 'Not Chronically\nHomeless']
        sizes = [chronically_homeless, total_count-chronically_homeless]
        explode = (.1, 0) 


        label_font = {'fontname':font_name, 'color':font_color, 'fontsize':font_size}
        fig, ax = plt.subplots(figsize=(width, height), dpi=image_dpi)

        patches, texts, pcts = ax.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%',wedgeprops={'linewidth': 1.0, 'edgecolor': 'white'},
            colors=colors, startangle=140, textprops=label_font)

        plt.setp(pcts, color=font_color, fontweight='bold',fontsize=data_label_font_size)


        filename = 'chplot.png'
        plt.savefig(os.path.join(chart_directory, title, filename), transparent=True, bbox_inches='tight')

        cropped_image = IMG.open(os.path.join(chart_directory, title, filename))
        pixels_per_inch = cropped_image.info['dpi'][0]
        pixels_to_trim = int(inches_to_trim * pixels_per_inch)
        crop_box = (pixels_to_trim, pixels_to_trim, cropped_image.width - pixels_to_trim, cropped_image.height - pixels_to_trim)
        cropped_image = cropped_image.crop(crop_box)
        cropped_image.save(os.path.join(chart_directory, title, filename))

        plt.close()


        ch_plot = get_image(os.path.join(chart_directory, title, filename),
                          width=3.75*inch)
        """





        insurance=m.insurance_status(**parameters)

        insurance_counts = list(insurance.values())
        percentages = [(cat_count / total_count)*100 for cat_count in insurance_counts]

        header_text = ["", "#", "%"]
        formatted_header_text = [Paragraph(cell, rf.tableSecondaryHeader) for cell in header_text]
        insurance_data = [formatted_header_text]
        for insur_cat, count in insurance.items():
            percentage = "{:.1%}".format(count/total_count)
            count = "{:,}".format(count)

            insurance_data.append([Paragraph(insur_cat, rf.tableTextStyle),
                                     Paragraph(count, rf.tableValuesStyle),
                                      Paragraph(percentage, rf.tableTargetStyle)])


        insure_table = Table(insurance_data,style=rf.demosTableStyle, colWidths=[1.75*inch,.65*inch,.5*inch], rowHeights=[inch/5]*len(insurance_data))

        dv_status=m.dv_status(**parameters)

        dv_counts = list(dv_status.values())

        header_text = ["", "#", "%"]
        formatted_header_text = [Paragraph(cell, rf.tableSecondaryHeader) for cell in header_text]
        dv_data = [formatted_header_text]
        for dv_cat, count in dv_status.items():
            percentage = "{:.1%}".format(count/total_count)
            count = "{:,}".format(count)

            dv_data.append([Paragraph(dv_cat, rf.tableTextStyle),
                                     Paragraph(count, rf.tableValuesStyle),
                                      Paragraph(percentage, rf.tableTargetStyle)])


        dv_table = Table(dv_data,style=rf.demosTableStyle, colWidths=[2.5*inch,.6*inch,.5*inch], rowHeights=[inch/5]*len(dv_data))
             
        #row1demotable = Table([[age_table, age_plot]],style=rf.demoPageTableStyle)
        page1_demographics = [[Paragraph("Age", rf.subSectionHeaderStyle),Spacer(0,inch/5)],[row1demotable],
                              [Paragraph("Race/Ethnicity", rf.subSectionHeaderStyle),Spacer(0,inch/5)],[row2demotable],                              
                              [Paragraph("Gender", rf.subSectionHeaderStyle),Spacer(0,inch/5)],[row3demotable]]
        page1_demographics_table = Table(page1_demographics, style=rf.demoPageTableStyle)
        self.elements.append(page1_demographics_table)
        self.elements.append(PageBreak())

        vetper = Paragraph("{:.1%}".format(m.veteran_status(**parameters)['Veteran']/total_count),rf.VetCHTextStyle)
        chper = Paragraph("{:.1%}".format(m.chronically_homeless_count(**parameters)/total_count),rf.VetCHTextStyle)
        

        vetpertable = Table([[vetper]],style=rf.VetStatusTableStyle,colWidths=[2*inch],rowHeights=[.6*inch])
        chpertable = Table([[chper]],style=rf.CHStatusTableStyle,colWidths=[2*inch],rowHeights=[.6*inch])


       
        vet_table = Table([[[Paragraph("Veterans", rf.subSectionHeaderStyle),Spacer(0,inch/4)]],[vetpertable]], style=rf.dismergeTableStyle)
        ch_table = Table([[[Paragraph("Chronically Homesless", rf.subSectionHeaderStyle),Spacer(0,inch/4)]],[chpertable]], style=rf.dismergeTableStyle)


        pg2row2_table = Table([[vet_table,ch_table]], style=rf.demoPageTableStyle)

        insure_merged_table = Table([[[Paragraph("Insurance Coverage", rf.subSectionHeaderStyle),Spacer(0,inch/4)]],[insure_table]], style=rf.dismergeTableStyle)
        dv_merged_table = Table([[[Paragraph("Domestic Violence", rf.subSectionHeaderStyle),Spacer(0,inch/4)]],[dv_table]], style=rf.dismergeTableStyle)
        
        pg2row3_table = Table([[insure_merged_table,dv_merged_table]], style=rf.dismergeTableStyle, colWidths=[3.75*inch]*2)

        
        page2_demographics =[[Paragraph("Disability", rf.subSectionHeaderStyle),Spacer(0,inch/4)],[disability_table],[pg2row2_table],[pg2row3_table]]      
        page2_demographics_table = Table(page2_demographics, style=rf.demoPage2TableStyle)
        self.elements.append(page2_demographics_table)  
        print(f"Finished Demographics for {title}")      
         
    def walkerGrid(self):    
        ##### NEEDS OVERHAUL #####
        
        ##### To-do #####
        # landscape
        # softcode dates
        # 0 values are displaying as "-"


        print(f"Starting Agency Indicators, All agency")

        self.elements.append(Paragraph("All Agency Indicators", rf.pageHeaderStyle))
        self.elements.append(Spacer(0, inch/4))

        q1_start_date = '2023-07-01'
        q1_end_date = '2023-09-30'
        q2_start_date = '2023-10-01'
        q2_end_date = '2023-12-31'
        fy_start_date = '2023-07-01'
        
        for ProgramType, IndicatorCategoryDict in self.agency_indicator_functions_dict.items():
            self.elements.append(Paragraph(ProgramType,rf.tableHeaderStyle))
            table_headers = ['Domain', 'Category', 'Indicator', 'Q1', '{self.cadence_short_name}', 'YTD', 'Target']
            table_headers_data = [[Paragraph(cell, rf.IndicSecondaryHeader) for cell in table_headers]] # set paragraph style for header cells 
            table_header_table = [Table(table_headers_data, style=rf.AgencyIndicatorHeaderStyle,
                                        colWidths=rf.walkerGridColWidths)]

            final_table_data = [[table_header_table]]

            for IndicatorCategoryName, IndicatorCategoryDict in self.agency_indicator_functions_dict[ProgramType].items():               
                cat_data = []
                for IndicatorName, IndicatorDict in IndicatorCategoryDict.items():           
                    if IndicatorDict['IndicatorFunction']:
                        q1_parameters = IndicatorDict['IndicatorParameter']
                        q2_parameters = IndicatorDict['IndicatorParameter']
                        ytd_parameters = IndicatorDict['IndicatorParameter']
                        
                        if ProgramType != 'All Agency':
                            q1_parameters.update({"program_type": [ProgramType]})
                            q2_parameters.update({"program_type": [ProgramType]})
                            ytd_parameters.update({"program_type": [ProgramType]})

                        q1_parameters.update({"start_date": q1_start_date, "end_date": q1_end_date})
                        q1_result = IndicatorDict['IndicatorFunction'](**q1_parameters)

                        q2_parameters.update({"start_date": q2_start_date, "end_date": q2_end_date})
                        q2_result = IndicatorDict['IndicatorFunction'](**q2_parameters)

                        ytd_parameters.update({"start_date": fy_start_date, "end_date": q2_end_date})
                        ytd_result = IndicatorDict['IndicatorFunction'](**ytd_parameters)

                        functionTarget = IndicatorDict['Format'].format(IndicatorDict['Target'])

                        format_value = IndicatorDict['Format']
                        if q1_result: 
                            formatted_q1_result = format_value.format(q1_result)
                        else:
                            formatted_q1_result = "-"                                  
                        
                        if q2_result: 
                            formatted_q2_result = format_value.format(q2_result)
                        else:
                            formatted_q2_result = "-"                            

                        if isinstance(ytd_result, numbers.Number):
                            if IndicatorDict['IndicatorType'] == 'larger':
                                if ytd_result < IndicatorDict['Target']:
                                    YTDfunctionResult = Paragraph(IndicatorDict['Format'].format(IndicatorDict['IndicatorFunction'](**ytd_parameters)), rf.tableValuesMissStyle)
                                else:
                                    YTDfunctionResult = Paragraph(IndicatorDict['Format'].format(IndicatorDict['IndicatorFunction'](**ytd_parameters)), rf.tableValuesExceedStyle)
                            else:
                                if ytd_result >= IndicatorDict['Target']:
                                    YTDfunctionResult = Paragraph(IndicatorDict['Format'].format(IndicatorDict['IndicatorFunction'](**ytd_parameters)), rf.tableValuesMissStyle)
                                else:
                                    YTDfunctionResult = Paragraph(IndicatorDict['Format'].format(IndicatorDict['IndicatorFunction'](**ytd_parameters)), rf.tableValuesExceedStyle)
                        else:
                            YTDfunctionResult = Paragraph("Coming Soon", rf.tableValuesStyle)

                        cat_data.append((Paragraph(IndicatorDict['IndicatorDomain'], rf.IndicNameTextStyle),
                                            Paragraph(IndicatorCategoryName, rf.IndicNameTextStyle),Paragraph(IndicatorName, rf.IndicNameTextStyle),
                                            Paragraph(formatted_q1_result, rf.IndicResult), Paragraph(formatted_q2_result, rf.IndicResult), 
                                            YTDfunctionResult, Paragraph(functionTarget, rf.tableTargetStyle)))
                if len(IndicatorCategoryName) > 1: # checks for categories without indicators
                    
                    cat_table = Table(cat_data, style=rf.AgencyIndicatorTableStyle, colWidths=rf.walkerGridColWidths,
                                        rowHeights=[inch/4]*(len(IndicatorCategoryDict)))
                else:
                    final_table_data = []
                    cat_table = ([[Paragraph("Coming Soon!", rf.subSectionHeaderStyle)],[Spacer(0,inch/4)]])
                final_table_data.append([cat_table])
            all_cat_table = Table(final_table_data, style=rf.AgencyIndicatorTableStructureStyle)
            self.elements.append(all_cat_table)
            self.elements.append(Spacer(0,inch/10))
            print(f"Loaded {ProgramType} tables for All Agency -  Agency Indicators, All agency")
                
        self.elements.append(PageBreak())        
        print(f"Finished Agency Indicators, All agency")

    def division(self, region=None, department=None):
        
        print(f"Starting Region/Department Summary for {region,department}")

        if region:
            self.elements.append(Paragraph(f"{region} Summary", rf.pageHeaderStyle))
            self.elements.append(Spacer(0,inch/6))
            
            print(f"   Starting Region/Department Indicators for {region,department}")
            self.indicators(region=region)
            self.elements.append(PageBreak())

            self.elements.append(Paragraph(f"{region} Demographics", rf.pageHeaderStyle))
            self.elements.append(Spacer(0,inch/10))

            self.elements.append(Paragraph("Fiscal Year-to-Date", rf.subSectionHeaderStyle))

            self.elements.append(Spacer(0,inch/6))

            print(f"   Starting Region/Department Demographics for {region,department}")
            self.demographics(region=region)
            self.elements.append(PageBreak())

            print(f"   Ending Region/Department Demographics for {region,department}")
        else:
            self.elements.append(Paragraph(f"{department} Summary", rf.pageHeaderStyle))
            self.elements.append(Spacer(0,inch/6))
            
            print(f"   Starting Region/Department Indicators for {region,department}")
            self.indicators(department=department)
            self.elements.append(PageBreak())

            self.elements.append(Paragraph(f"{department} Demographics", rf.pageHeaderStyle))
            self.elements.append(Spacer(0,inch/6))

            print(f"   Starting Region/Department Demographics for {region,department}")
            self.demographics(department=department)
            self.elements.append(PageBreak())

            print(f"   Ending Region/Department Demographics for {region,department}")


        print(f"Ending Region/Department Summary for {region,department}")

    def agencyIndicators(self, parameters, ytd_parameters=None):
        self.elements.append(Paragraph(f"Agency Indicators", rf.subSectionHeaderStyle))
        self.elements.append(Spacer(10, inch/6))
        
        ytd_parameters = ytd_parameters.copy()  
     
        ActiveClientsFullPeriod = "{:,}".format(m.active_clients(**parameters))                          
        ActiveHouseholdsFullPeriod = "{:,}".format(m.active_household(**parameters))
        NewParticipants = "{:,}".format(m.new_clients(**parameters))
        NewHousholds = "{:,}".format(m.new_household(**parameters))        
        
        YTDActiveClientsFullPeriod = "{:,}".format(m.active_clients(**ytd_parameters))                          
        YTDActiveHouseholdsFullPeriod = "{:,}".format(m.active_household(**ytd_parameters))
        YTDNewParticipants = "{:,}".format(m.new_clients(**ytd_parameters))
        YTDNewHousholds = "{:,}".format(m.new_household(**ytd_parameters))

        persons_served_header = [Paragraph("Persons Served",rf.tableHeaderStyle),"",""]
        total_served_header = [Paragraph("Total Served\u00B9",rf.tableSubHeader),Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader)] 
        total_served_row1 = [Paragraph("Participants",rf.tableTextStyle),Paragraph(ActiveClientsFullPeriod,rf.tableValuesStyle), Paragraph(YTDActiveClientsFullPeriod,rf.tableValuesStyle)]
        total_served_row2 = [Paragraph("Households",rf.tableTextStyle), Paragraph(ActiveHouseholdsFullPeriod,rf.tableValuesStyle), Paragraph(YTDActiveHouseholdsFullPeriod,rf.tableValuesStyle)]
        new_enrollments_titles = [Paragraph("New Enrollments\u00B2",rf.tableSubHeader), "", ""]
        new_enrollments_row1 = [Paragraph("Participants",rf.tableTextStyle),Paragraph(NewParticipants,rf.tableValuesStyle),Paragraph(YTDNewParticipants,rf.tableValuesStyle)]
        new_enrollments_row2 = [Paragraph("Households",rf.tableTextStyle),Paragraph(NewHousholds,rf.tableValuesStyle),Paragraph(YTDNewHousholds,rf.tableValuesStyle)]
        
        persons_served_footer_text = f"1. Active at anytime between {self.formatted_start_date} and {self.formatted_end_date}.<br/>2. Program start date anytime between {self.formatted_start_date} and {self.formatted_end_date}."
        persons_served_footer = [Paragraph(persons_served_footer_text,rf.tableFooterStyle)]
        
        persons_served_data = [persons_served_header, 
                               total_served_header, total_served_row1, total_served_row2,
                               new_enrollments_titles, new_enrollments_row1, new_enrollments_row2,
                                persons_served_footer]

        PersonsServedTable = Table(persons_served_data, style=rf.PersonsServedTableStyle, colWidths=[1.5*inch,.6*inch,.6*inch],
                                    rowHeights=rf.allIndicatorsRow1Height)

        print("   Loaded Persons Served")
                
        # Permanent Housing Placements
        TotalPHPs = "{:,}".format(m.total_php_count(**parameters))
        MoveInPHPs = "{:,}".format(m.movein_php_count(**parameters))
        ExitsToPermPHPs = "{:,}".format(m.exit_to_perm_php_count(**parameters))

        TYDTotalPHPs = "{:,}".format(m.total_php_count(**ytd_parameters))
        TYDMoveInPHPs = "{:,}".format(m.movein_php_count(**ytd_parameters))
        TYDExitsToPermPHPs = "{:,}".format(m.exit_to_perm_php_count(**ytd_parameters))

        php_header = [Paragraph("Permanent Housing Placements",rf.tableHeaderStyle),"", ""]
        php_title = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader)] 
        php_row1 = [Paragraph("Direct Placements",rf.tableTextStyle), Paragraph(MoveInPHPs,rf.tableValuesStyle),Paragraph(TYDMoveInPHPs,rf.tableValuesStyle)]
        php_row2 = [Paragraph("Exits to Permanent Destinations",rf.tableTextStyle),Paragraph(ExitsToPermPHPs,rf.tableValuesStyle),Paragraph(TYDExitsToPermPHPs,rf.tableValuesStyle)]
        php_row3 = [Paragraph("Total Permanent Housing Placements",rf.tableTextStyle),Paragraph(TotalPHPs,rf.tableValuesStyle),Paragraph(TYDTotalPHPs,rf.tableValuesStyle)]

        phps_data = [php_header, php_title, php_row1, php_row2, php_row3,"","",""]

        PHPTable = Table(phps_data, style=rf.PHPTableStyle,
                            colWidths=[2*inch,.6*inch,.6*inch], rowHeights=rf.allIndicatorsRow1Height)

        print("   Loaded PHP")
        
        # Days to First Service
        parameters.update({"days":3})
        ServedIn3Days = "{:.1%}".format(m.served_within_x_days(**parameters))
        parameters.update({"days":7})
        ServedIn7Days = "{:.1%}".format(m.served_within_x_days(**parameters))
        parameters.popitem()        
        
        ytd_parameters.update({"days":3})
        YTDServedIn3Days = "{:.1%}".format(m.served_within_x_days(**ytd_parameters))
        ytd_parameters.update({"days":7})
        YTDServedIn7Days = "{:.1%}".format(m.served_within_x_days(**ytd_parameters))
        ytd_parameters.popitem()

        served_header = [Paragraph("Days to First Service\u00B3",rf.tableHeaderStyle),"",""]
        served_title = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader)] 
        served_row1 = [Paragraph("Served within 3 days",rf.tableTextStyle),Paragraph(ServedIn3Days,rf.tableValuesStyle),Paragraph(YTDServedIn3Days,rf.tableValuesStyle)]
        served_row2 = [Paragraph("Served within 7 days",rf.tableTextStyle),Paragraph(ServedIn7Days,rf.tableValuesStyle),Paragraph(YTDServedIn7Days,rf.tableValuesStyle)]
        served_row3 = []
        served_footer = [Paragraph("3. Service data from HMIS only.",rf.tableFooterStyle)]

        served_data = [served_header, served_title, served_row1, served_row2, served_row3, served_footer]

        ServedTable = Table(served_data, style=rf.PHPTableStyle,
                            colWidths=[1.15*inch,.5*inch,.5*inch], rowHeights=rf.allIndicatorsRow2Height)

        print("   Loaded Days to service")


        # Income Increases
        AnyIncome = "{:,}".format(0)
        EarnedIncome = "{:,}".format(0)
        BenefitIncome = "{:,}".format(0)
        
        YTDAnyIncome = "{:,}".format(0)
        YTDEarnedIncome = "{:,}".format(0)
        YTDBenefitIncome = "{:,}".format(0)
        income_header = [Paragraph("Participants with Income Increase\u2074",rf.tableHeaderStyle), "", ""]
        income_title = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader)] 

        income_row1 = [Paragraph("Earned Income Increase",rf.tableTextStyle), Paragraph(EarnedIncome,rf.tableValuesStyle),Paragraph(YTDEarnedIncome,rf.tableValuesStyle)]
        income_row2 = [Paragraph("Benefit and Other Income Increase",rf.tableTextStyle), Paragraph(BenefitIncome,rf.tableValuesStyle),Paragraph(YTDBenefitIncome,rf.tableValuesStyle)]
        income_row3 = [Paragraph("Any Income\u2075",rf.tableTextStyle), Paragraph(AnyIncome,rf.tableValuesStyle),Paragraph(YTDAnyIncome,rf.tableValuesStyle)]
        income_footer_text = f"4. Income entered prior to {self.formatted_end_date}.<br/>5. The total count for Any Income may not match the sum of Earned Income and Benefit Income due to income adjustments in other categories."
        income_footer = [Paragraph(income_footer_text,rf.tableFooterStyle)]

        income_increases_data = [income_header, income_title, income_row1, income_row2, income_row3, income_footer]
        
        IncomeIncreaseTable = Table(income_increases_data, style=rf.IncomeIncreaseTableStyle,
                    colWidths=[1.95*inch,.5*inch,.5*inch], rowHeights=rf.allIndicatorsRow2Height)
        
        print("   Loaded Income Increases")

        
        # Data Quality
        
        PIIQuality = "-"
        PIIresult = m.personal_data_quality(**parameters)
        if PIIresult:    
            PIIQuality = "{:.1%}".format(PIIresult)        

        UniversalQuality = "-"
        Universalresult = 0
        if Universalresult:    
            UniversalQuality = "{:.1%}".format(Universalresult)
        
        YTDPIIQuality = "-"
        YTDPIIresult = m.personal_data_quality(**ytd_parameters)
        if YTDPIIresult:    
            YTDPIIQuality = "{:.1%}".format(YTDPIIresult)        

        YTDUniversalQuality = "-"
        YTDUniversalresult = 0
        if YTDUniversalresult:    
            YTDUniversalQuality = "{:.1%}".format(YTDUniversalresult)
        
        dq_header = [Paragraph("Data Quality Score",rf.tableHeaderStyle)]
        dq_title = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader)] 
        dq_row1 = [Paragraph("Personal Identifiable Info",rf.tableTextStyle),Paragraph(PIIQuality,rf.tableValuesStyle),Paragraph(YTDPIIQuality,rf.tableValuesStyle)]
        dq_row2 = [Paragraph("Universal Elements",rf.tableTextStyle),Paragraph(UniversalQuality,rf.tableValuesStyle),Paragraph(YTDUniversalQuality,rf.tableValuesStyle)]
        dq_row3 = []
        dq_footer = []
        
        #[f"{dq_footer_text1}\n{dq_footer_text2}", ""]

        data_quality_data = [dq_header, dq_title, dq_row1, dq_row2, dq_row3, dq_footer]


        DataQualityTable = Table(data_quality_data, style=rf.PHPTableStyle,
                    colWidths=[1.4*inch,.5*inch,.5*inch], rowHeights=rf.allIndicatorsRow2Height)
        


        print("   Loaded Data quality")

        row1spacertable = Table([[""],[""],[""],[""],[""],[""],[""],[""]],style=rf.Row1SpacerStyle,
                                colWidths=[1.6*inch], rowHeights=rf.allIndicatorsRow1Height)
       
        
        ProgramLevelAgencyIndicatorsTableDataRow1 = [PersonsServedTable,row1spacertable,PHPTable]

        ProgramLevelAgencyIndicatorsTableRow1 = Table([ProgramLevelAgencyIndicatorsTableDataRow1],
                                                  style=rf.ProgramLevelAgencyIndicatorsAlignmentTableStyle)
        
        ProgramLevelAgencyIndicatorsTableDataRow2 = [ServedTable,IncomeIncreaseTable,DataQualityTable]

        ProgramLevelAgencyIndicatorsTableRow2 = Table([ProgramLevelAgencyIndicatorsTableDataRow2],
                                                  style=rf.ProgramLevelAgencyIndicatorsAlignmentTableStyle)
        

        ProgramLevelAgencyIndicatorsTable = Table([[ProgramLevelAgencyIndicatorsTableRow1], [ProgramLevelAgencyIndicatorsTableRow2]],
                                                  style=rf.ProgramLevelAgencyIndicatorsAlignmentTableStyle)
        

        self.elements.append(ProgramLevelAgencyIndicatorsTable)
        print(f"Added Agency Indicators")
            
    def contractIndicators(self, program_type, program_id, grant_dict):
        self.elements.append(Spacer(0,inch/12))
        self.elements.append(Paragraph(f"Contract Indicators", rf.subSectionHeaderStyle))

        for grant_code, contract_term in grant_dict.items():
            reference_parameters = ({"start_date": self.fy_start_date, "end_date": self.end_date})

            IndicatorDict = self.pullInidcatorDict(grant_code)
            
            grant_code_header = [f"Grant code: {grant_code}",contract_term,"",""]
            indicator_header = ["", "FYTD Result", "Target", "Reporting Cadence"]

            grant_code_header = [[Paragraph(cell, rf.KPItableHeaderStyle) for cell in grant_code_header]]
            indicator_header = [[Paragraph(cell, rf.KPItableSecondaryHeader) for cell in indicator_header]]
            
            grant_table_data = grant_code_header + indicator_header
            try: 
                for IndicatorName, IndicatorDict in IndicatorDict.items():    
                    parameters = reference_parameters.copy()
                    contract_name = IndicatorDict['ContractName'] 
                    reporting_cadence = "Not Specified"
                    
                    if IndicatorDict['ReportingCadence']:
                        reporting_cadence = IndicatorDict['ReportingCadence'] 
                    
                    if IndicatorDict['IndicatorFunction'] or IndicatorDict['IndicatorFunction'] is not None:

                        update_dict = IndicatorDict['IndicatorParameter']
                        update_dict["program_id"] = [program_id]
                        parameters.update(update_dict)

                        result = IndicatorDict['IndicatorFunction'](**parameters)
                        format_value = IndicatorDict['Format']
                        target = IndicatorDict['IndicatorTarget']
                        if isinstance(result, numbers.Number): 
                            formatted_result = format_value.format(result)
                            formattted_target = format_value.format(target)
                        else:
                            formatted_result = "-"
                            formattted_target = "-"
                        
                        grant_table_data.append([Paragraph(IndicatorName, rf.KPItableIndcatorStyle),
                                                Paragraph(formatted_result, rf.KPItableValuesStyle),
                                                Paragraph(formattted_target, rf.KPItableTargetStyle),
                                                Paragraph(reporting_cadence, rf.KPItableCadenceStyle)])
                    else:
                        format_value = IndicatorDict['Format']
                        target = IndicatorDict['IndicatorTarget']
                        if formattted_target.isnumeric:
                            format_value.format(target)
                        grant_table_data.append([Paragraph(IndicatorName, rf.KPItableIndcatorStyle),
                                                Paragraph("TBD", rf.KPItableValuesStyle),
                                                Paragraph(formattted_target, rf.KPItableTargetStyle),
                                                Paragraph(reporting_cadence, rf.KPItableCadenceStyle)])   
            except:
                AttributeError
            
            grantTable = Table(grant_table_data, style=rf.ProgramLevelContractIndicatorsTableStyle, colWidths=[None,.7*inch,.7*inch,.7*inch])
            self.elements.append(grantTable)
                      
    def programPage(self, program_type, program_name, program_dict):
        
        self.elements.append(Paragraph(program_name, rf.programHeaderStyle))
        self.elements.append(Spacer(0,inch/12))

        for MergedID, MergedIDData in program_dict.items():
            # Program information header
            DataSystemProgramName = MergedIDData['DataSystemProgramName']
            PrimaryDataSystem = MergedIDData['PrimaryDataSystem']
            DataSystemID = MergedIDData['DataSystemID']
                                
            program_info = ["Data system name",DataSystemProgramName, "Primary data system",PrimaryDataSystem, "Data system ID", DataSystemID]
            program_info_table = Table([program_info], style=rf.progInfoTableStyle, colWidths=rf.colummWidths(PrimaryDataSystem), rowHeights=[inch/6])

            self.elements.append(program_info_table)
            self.elements.append(Spacer(0,inch/8))
            print("Loaded header:", MergedID)
            
            self.indicators(MergedID)
            #self.contractIndicators(program_type, MergedID, MergedIDData['ContractTerm'])

            self.elements.append(PageBreak())
       
    def outreachIndicators(self, parameters, ytd_parameters=None):
        allPageData = []

        reference_parameters = parameters.copy()
        ytd_reference_parameters = ytd_parameters.copy()  

        indicatorTableData = []

        # Participant Engagement
        indicatorNameOriginal = "% Engaged" 

        catName, indicatorName, YTDfunctionResult, functionTarget, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_reference_parameters)
        catName, indicatorName, functionResult, functionTarget, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, reference_parameters)

        functionHeaderRow = [catName,"","",""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Target", rf.tableSecondaryHeader)]
        functionRow1 = [indicatorName, functionResult, YTDfunctionResult, functionTarget]
        functionFooterRow = [functionFooter]

        functionRow2 = []
        functionRow3 = []
        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2, functionRow3]
        functionTable = Table(tableData, style=rf.SingleIndicatorTCondensedableStyle,
                               colWidths=[.75*inch, .5*inch, .5*inch, .75*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)
               
        # Exit Destinations
        # Exits to Positive Destinations
        indicatorNameOriginal = "% of Exits to Positive Destinations" 

        catName, indicatorName, YTDfunctionResult1, functionTarget, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_reference_parameters)
        catName, indicatorName1, functionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, reference_parameters)
        functionRow1 = [indicatorName1, functionResult1,YTDfunctionResult1, functionTarget1]

        # Exits to Permanent Destinations
        indicatorNameOriginal = "% of Exits to Permanent Destinations" 

        catName, indicatorName, YTDfunctionResult2, functionTarget, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_reference_parameters)
        catName, indicatorName2, functionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, reference_parameters)
        functionRow2 = [indicatorName2, functionResult2, YTDfunctionResult2, functionTarget2]

        # Exits to Homelessness
        indicatorNameOriginal = "% of Exits to Homelessness" 

        catName, indicatorName, YTDfunctionResult3, functionTarget, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_reference_parameters)
        catName, indicatorName3, functionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, reference_parameters)
        functionRow3 = [indicatorName3, functionResult3, YTDfunctionResult3, functionTarget3]

        total_exits = "{:,}".format(m.total_exits(**parameters))   

        functionHeaderRow = [catName, "", Paragraph(f"{self.cadence_short_name} Exits: {total_exits}", rf.tableTextStyle),""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Target", rf.tableSecondaryHeader)]
        functionFooterRow = [functionFooter]

        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2, functionRow3]
        functionTable = Table(tableData, style=rf.TwoOrMoreIndicatorCondensedTableStyle,
                               colWidths=[2*inch, .5*inch, .5*inch, .75*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)

        row1spacertable = Table([[""]]*len(tableData),style=rf.Row1SpacerCondensedStyle,
                        colWidths=[1.25*inch], rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))

        indicatorTableData.insert(1, row1spacertable)
        
        indicatorTable = Table([indicatorTableData], style=rf.ProgTypeIndicatorsAlignmentTableStyle)
        
        allPageData.append([indicatorTable])
        allPageTable = Table(allPageData, style=rf.ProgTypeIndicatorsAlignmentTableStyle)  
        self.elements.append(allPageTable)
        
    def interimIndicators(self, parameters, ytd_parameters=None):
        allPageData = []
        
        reference_parameters = copy.deepcopy(parameters)
        ytd_reference_parameters = copy.deepcopy(ytd_parameters)

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        indicatorTableData = []

        # Site Utilization
        indicatorNameOriginal = "Utilization Rate" 
        
        catName, indicatorName, YTDfunctionResult, functionTarget, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName, functionResult, functionTarget, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)

        functionHeaderRow = [catName,"","",""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionRow1 = [indicatorName, functionResult, YTDfunctionResult, functionTarget]
        functionFooterRow = [functionFooter]


        functionRow2 = []
        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2]
        functionTable = Table(tableData, style=rf.SingleIndicatorTCondensedableStyle,
                               colWidths=[1*inch, .5*inch, .5*inch, .75*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)

        # Exit Destinations
        # Exits to Permanent Destinations
        indicatorNameOriginal = "% of Exits to Permanent Destinations" 

        catName, indicatorName1, YTDfunctionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName1, functionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow1 = [indicatorName1, functionResult1, YTDfunctionResult1, functionTarget1]

        # Exits to Homelessness
        indicatorNameOriginal = "% of Exits to Homelessness" 

        catName, indicatorName2, YTDfunctionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName2, functionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow2 = [indicatorName2, functionResult2, YTDfunctionResult2, functionTarget2]

        total_exits = "{:,}".format(m.total_exits(**parameters))

        functionHeaderRow = [catName, "", Paragraph(f"{self.cadence_short_name} Exits: {total_exits}", rf.tableTextStyle),""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionFooterRow = [functionFooter]

        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2]
        functionTable = Table(tableData, style=rf.TwoOrMoreIndicatorCondensedTableStyle,
                               colWidths=[2*inch, .5*inch, .5*inch, .75*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)

        row1spacertable = Table([[""]]*len(tableData),style=rf.Row1SpacerCondensedStyle,
                        colWidths=[1*inch], rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))

        indicatorTableData.insert(1, row1spacertable)

        indicatorTable = Table([indicatorTableData], style=rf.ProgTypeIndicatorsAlignmentTableStyle)

        allPageData.append([indicatorTable])
        allPageData.append([Spacer(0,inch/10)])

        # Row 2
        indicatorTableData = []

        # Length of Stay
        # Average Length of Stay
        indicatorNameOriginal = "Average Length of Stay" 

        catName, indicatorName1, YTDfunctionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName1, functionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow1 = [indicatorName1, functionResult1,YTDfunctionResult1, functionTarget1]


        # Median Length of Stay
        indicatorNameOriginal = "Median Length of Stay" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName2, YTDfunctionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName2, functionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow2 = [indicatorName2, functionResult2,YTDfunctionResult2, functionTarget2]

        functionHeaderRow = [catName, "","",""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionFooterRow = [functionFooter]

        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2, functionFooterRow]
        functionTable = Table(tableData, style=rf.TwoOrMoreIndicatorTableStyle,
                               colWidths=[1.25*inch, .5*inch, .5*inch, .75*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)



        # Days to Permanent Destination
        # Average Days to Permanent Destination
        indicatorNameOriginal = "Average Days to Permanent Destination" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName1, YTDfunctionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName1, functionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow1 = [indicatorName1, functionResult1,YTDfunctionResult1, functionTarget1]

        
        # Median Days to Permanent Destination
        indicatorNameOriginal = "Median Days to Permanent Destination" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName2, YTDfunctionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName2, functionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow2 = [indicatorName2, functionResult2,YTDfunctionResult2, functionTarget2]

        functionHeaderRow = [catName, "","",""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionFooterRow = [functionFooter]

        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2, functionFooterRow]
        functionTable = Table(tableData, style=rf.TwoOrMoreIndicatorTableStyle,
                               colWidths=[2.1*inch, .5*inch, .5*inch, .75*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)
        
        row1spacertable = Table([[""]]*len(tableData),style=rf.Row1SpacerStyle,
                        colWidths=[.65*inch], rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))

        indicatorTableData.insert(1, row1spacertable)
        
        indicatorTable = Table([indicatorTableData], style=rf.ProgTypeIndicatorsAlignmentTableStyle)

        allPageData.append([indicatorTable])
        allPageData.append([Spacer(0,inch/10)])

        # Row 3
        indicatorTableData = []
        # Placement Preparations
        indicatorNameOriginal = "% Document Ready" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName, YTDfunctionResult, functionTarget, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName, functionResult, functionTarget, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)

        functionHeaderRow = [catName,"","",""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionRow1 = [indicatorName, functionResult, YTDfunctionResult, functionTarget]
        functionFooterRow = [functionFooter]

        functionRow2 = []
        tableData = [functionHeaderRow, functionTitleRow, functionRow1]
        functionTable = Table(tableData, style=rf.SingleIndicatorTCondensedableStyle,
                               colWidths=[1.25*inch, .5*inch, .5*inch, .75*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        

        indicatorTableData.append(functionTable)

        row1spacertable = Table([[""]]*len(tableData),style=rf.Row1SpacerCondensedStyle,
                        colWidths=[4.5*inch], rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))

        indicatorTableData.insert(1, row1spacertable)
        
        indicatorTable = Table([indicatorTableData], style=rf.ProgTypeIndicatorsAlignmentTableStyle)

        allPageData.append([indicatorTable])
        allPageTable = Table(allPageData, style=rf.ProgTypeIndicatorsAlignmentTableStyle)  
        self.elements.append(allPageTable)

    def housingNavIndicators(self, parameters, ytd_parameters=None):
        allPageData = []

        reference_parameters = parameters.copy()
        ytd_reference_parameters = ytd_parameters.copy()  


        # Row 1
        indicatorTableData = []

        # Days to Permanent Placement
        # Average Days to Permanent Placement
        indicatorNameOriginal = "Average Days to Permanent Placement" 

        catName, indicatorName1, YTDfunctionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName1, functionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow1 = [indicatorName1, functionResult1, YTDfunctionResult1, functionTarget1]

        
        # Median Days to Permanent Placement
        indicatorNameOriginal = "Median Days to Permanent Placement" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName2, YTDfunctionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName2, functionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow2 = [indicatorName2, functionResult2, YTDfunctionResult2, functionTarget2]

        functionHeaderRow = [catName,"","",""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionFooterRow = [functionFooter]

        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2]
        functionTable = Table(tableData, style=rf.TwoOrMoreIndicatorCondensedTableStyle,
                               colWidths=[2.1*inch, .45*inch, .45*inch, .70*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)
        

        # Exit Destinations
        # Exits to Permanent Destinations
        indicatorNameOriginal = "% of Exits to Permanent Destinations" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName1, YTDfunctionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName1, functionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow1 = [indicatorName1, functionResult1, YTDfunctionResult1, functionTarget1]

        # Exits to Homelessness
        indicatorNameOriginal = "% of Exits to Homelessness" 

        catName, indicatorName2, YTDfunctionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        catName, indicatorName2, functionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow2 = [indicatorName2, functionResult2, YTDfunctionResult2, functionTarget2]

        total_exits = "{:,}".format(m.total_exits(**parameters))

        functionHeaderRow = [catName,"",Paragraph(f"{self.cadence_short_name} Exits: {total_exits}", rf.tableTextStyle), ""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionFooterRow = [functionFooter]

        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2]
        functionTable = Table(tableData, style=rf.TwoOrMoreIndicatorCondensedTableStyle,
                               colWidths=[2*inch, .45*inch, .45*inch, .75*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)

        row1spacertable = Table([[""]]*len(tableData),style=rf.Row1SpacerCondensedStyle,
                        colWidths=[.15*inch], rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))

        indicatorTableData.insert(1, row1spacertable)

        indicatorTable = Table([indicatorTableData], style=rf.ProgTypeIndicatorsAlignmentTableStyle)

        allPageData.append([indicatorTable])
        allPageData.append([Spacer(0,inch/10)])

        # Row 2
        indicatorTableData = []
        # Placement Preparations
        # % Document Ready
        indicatorNameOriginal = "% Document Ready" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName1, YTDfunctionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName1, functionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow1 = [indicatorName1, functionResult1, YTDfunctionResult1, functionTarget1]

        # % with HSP in 30
        indicatorNameOriginal = "% with HSP within 30 Days" 
        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()
        
        catName, indicatorName2, YTDfunctionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName2, functionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow2 = [indicatorName2, functionResult2,YTDfunctionResult2, functionTarget2]

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()
        total_exits = "{:,}".format(m.total_exits(**parameters))

        functionHeaderRow = [catName,"", "",""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionFooterRow = [functionFooter]

        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2]
        functionTable = Table(tableData, style=rf.TwoIndicatorCondensedTableStyle,
                               colWidths=[2.5*inch, .5*inch, .5*inch, .75*inch],
                               rowHeights=[inch/4,inch/4,inch/3,inch/3])
        
        indicatorTableData.append(functionTable)

        row1spacertable = Table([[""]]*len(tableData),style=rf.Row1SpacerCondensedStyle,
                        colWidths=[3.25*inch], rowHeights=[inch/4,inch/4,inch/3,inch/3])

        indicatorTableData.insert(1, row1spacertable)

        indicatorTable = Table([indicatorTableData], style=rf.ProgTypeIndicatorsAlignmentTableStyle)

        allPageData.append([indicatorTable])
        allPageTable = Table(allPageData, style=rf.ProgTypeIndicatorsAlignmentTableStyle)  
        self.elements.append(allPageTable)

    def rapidIndicators(self, parameters, ytd_parameters=None):
        allPageData = []

        reference_parameters = parameters.copy()
        ytd_reference_parameters = ytd_parameters.copy()  

        # Row 1
        indicatorTableData = []

        # Days to Permanent Placement
        # Average Days to Move-In
        indicatorNameOriginal = "Average Days to Move-In" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName1, YTDfunctionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName1, functionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow1 = [indicatorName1, functionResult1,YTDfunctionResult1, functionTarget1]

        
        # Median Days to Move-In
        indicatorNameOriginal = "Median Days to Move-In" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName2, YTDfunctionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName2, functionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow2 = [indicatorName2, functionResult2,YTDfunctionResult2, functionTarget2]

        functionHeaderRow = [catName,"","",""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionFooterRow = [functionFooter]

        functionRow3 = []
        functionRow4 = []
        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2, functionRow3, functionRow4]
        functionTable = Table(tableData, style=rf.TwoIndicatorCondensedTableStyle,
                               colWidths=[1.5*inch, .5*inch, .5*inch, .75*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)


        # Timeliness of Placement
        # % Housed within 30 Days
        indicatorNameOriginal = "% Housed within 30 days" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName1, YTDfunctionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName1, functionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow1 = [indicatorName1, functionResult1,YTDfunctionResult1, functionTarget1]

        
        # % Housed within 60 Days
        indicatorNameOriginal = "% Housed within 60 days" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName2, YTDfunctionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName2, functionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow2 = [indicatorName2, functionResult2,YTDfunctionResult2, functionTarget2]        
        
        # % Housed within 90 Days
        indicatorNameOriginal = "% Housed within 90 days" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName3, YTDfunctionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName3, functionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow3 = [indicatorName3, functionResult3,YTDfunctionResult3, functionTarget3]

        
        # % Housed within 120 Days
        indicatorNameOriginal = "% Housed within 120 days" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName4, YTDfunctionResult4, functionTarget4, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName4, functionResult4, functionTarget4, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow4 = [indicatorName4, functionResult4,YTDfunctionResult4, functionTarget4]

        parameters = reference_parameters.copy()
        total_placed = "{:,}".format(m.movein_php_count(**parameters))

        functionHeaderRow = [catName, "",Paragraph(f"{self.cadence_short_name} Placements: {total_placed}", rf.tableTextStyle),""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionFooterRow = [functionFooter]

        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2, functionRow3, functionRow4]
        functionTable = Table(tableData, style=rf.TwoOrMoreIndicatorCondensedTableStyle,
                               colWidths=[1.4*inch, .5*inch, .5*inch, .75*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)

        row1spacertable = Table([[""]]*len(tableData),style=rf.Row1SpacerCondensedStyle,
                        colWidths=[1.1*inch], rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))

        indicatorTableData.insert(1, row1spacertable)

        indicatorTable = Table([indicatorTableData], style=rf.ProgTypeIndicatorsAlignmentTableStyle)
       
        allPageData.append([indicatorTable])
        allPageData.append([Spacer(0,inch/10)])


        # Row 2
        indicatorTableData = []

        # Housing Retention
        # Housing Retention Rate - 3 Months
        indicatorNameOriginal = "Housing Retention Rate - 3 Months" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName1, YTDfunctionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName1, functionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow1 = [indicatorName1, functionResult1,YTDfunctionResult1, functionTarget1]

        # Housing Retention Rate - 6 Months
        indicatorNameOriginal = "Housing Retention Rate - 6 Months" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName2, YTDfunctionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName2, functionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow2 = [indicatorName2, functionResult2,YTDfunctionResult2, functionTarget2]        
        
        # Housing Retention Rate - 9 Months
        indicatorNameOriginal = "Housing Retention Rate - 9 Months" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName3, YTDfunctionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName3, functionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow3 = [indicatorName3, functionResult3,YTDfunctionResult3, functionTarget3]

        # Housing Retention Rate - 12 Months
        indicatorNameOriginal= "Housing Retention Rate - 12 Months" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName4, YTDfunctionResult4, functionTarget4, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName4, functionResult4, functionTarget4, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow4 = [indicatorName4, functionResult4,YTDfunctionResult4, functionTarget4]

        # Housing Retention Rate - 18 Months
        indicatorNameOriginal= "Housing Retention Rate - 18 Months" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName5, YTDfunctionResult5, functionTarget5, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName5, functionResult5, functionTarget5, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow5 = [indicatorName5, functionResult5,YTDfunctionResult5, functionTarget5]

        # Housing Retention Rate - 24 Months
        indicatorNameOriginal= "Housing Retention Rate - 24 Months" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName6, YTDfunctionResult6, functionTarget6, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName6, functionResult6, functionTarget6, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow6 = [indicatorName6, functionResult6,YTDfunctionResult6, functionTarget6]

        functionHeaderRow = [catName,"","",""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionFooterRow = [functionFooter]

        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2, functionRow3, functionRow4, functionRow5, functionRow6]
        functionTable = Table(tableData, style=rf.TwoOrMoreIndicatorCondensedTableStyle,
                               colWidths=[1.9*inch, .5*inch, .5*inch, .7*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)

        # Income Increases
        # % with Any Income Increase
        indicatorNameOriginal = "% with Any Income Increase" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName1, YTDfunctionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName1, functionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow1 = [indicatorName1, functionResult1,YTDfunctionResult1, functionTarget1]

        # % with Earned Income Increase
        indicatorNameOriginal = "% with Earned Income Increase" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName2, YTDfunctionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName2, functionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow2 = [indicatorName2, functionResult2,YTDfunctionResult2, functionTarget2]

        # % with Benefit and Other Income Increase
        indicatorNameOriginal = "% with Benefit and Other Income Increase" 
   
        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName3, YTDfunctionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName3, functionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow3 = [indicatorName3, functionResult3,YTDfunctionResult3, functionTarget3]
        
        functionHeaderRow = [catName, "","",""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionFooterRow = [functionFooter]

        functionRow4 = []
        functionRow5 = []
        functionRow6 = []
        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2, functionRow3, functionRow4, functionRow5, functionRow6]
        functionTable = Table(tableData, style=rf.TwoIndicatorCondensedTableStyle,
                               colWidths=[2.2*inch, .5*inch, .5*inch, .7*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)

        row1spacertable = Table([[""]]*len(tableData),style=rf.Row1SpacerCondensedStyle,
                        colWidths=[.1*inch], rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))

        #indicatorTableData.insert(1, row1spacertable)

        indicatorTable = Table([indicatorTableData], style=rf.ProgTypeIndicatorsAlignmentTableStyle)
        
        allPageData.append([indicatorTable])
        allPageData.append([Spacer(0,inch/10)])

        # Row 3
        indicatorTableData = []

        # Placement Preparations
        # % Document Ready
        indicatorNameOriginal = "% Document Ready" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName1, YTDfunctionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName1, functionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow1 = [indicatorName1, functionResult1, YTDfunctionResult1,functionTarget1]

        # % with HSP in 30
        indicatorNameOriginal = "% with HSP within 30 Days" 
        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName2, YTDfunctionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName2, functionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow2 = [indicatorName2, functionResult2,YTDfunctionResult2, functionTarget2]

        functionHeaderRow = [catName,"", "",""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionFooterRow = [functionFooter]

        functionRow3 = []
        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2, functionRow3]
        functionTable = Table(tableData, style=rf.TwoIndicatorCondensedTableStyle,
                               colWidths=[1.8*inch, .5*inch, .5*inch, .75*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)

        # Exit Destinations
        # % of Exits to Permanent Destinations
        indicatorNameOriginal = "% of Exits to Permanent Destinations" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName1, YTDfunctionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName1, functionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow1 = [indicatorName1, functionResult1,YTDfunctionResult1, functionTarget1]

        # % of Exits to Non-Permanent Destinations
        indicatorNameOriginal = "% of Exits to Non-Permanent Destinations" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName2, YTDfunctionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName2, functionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow2 = [indicatorName2, functionResult2,YTDfunctionResult2, functionTarget2]

        # Exits to Homelessness
        indicatorNameOriginal = "% of Exits to Homelessness" 

        catName, indicatorName3, YTDfunctionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName3, functionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow3 = [indicatorName3, functionResult3,YTDfunctionResult3, functionTarget3]

        total_exits = "{:,}".format(m.total_exits(**parameters))

        functionHeaderRow = [catName, "",Paragraph(f"{self.cadence_short_name} Exits: {total_exits}", rf.tableTextStyle),""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionFooterRow = [functionFooter]    

        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2, functionRow3]
        functionTable = Table(tableData, style=rf.TwoOrMoreIndicatorCondensedTableStyle,
                               colWidths=[2.2*inch, .5*inch, .5*inch, .75*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)


        indicatorTable = Table([indicatorTableData], style=rf.ProgTypeIndicatorsAlignmentTableStyle)

        allPageData.append([indicatorTable])
        allPageTable = Table(allPageData, style=rf.ProgTypeIndicatorsAlignmentTableStyle)  
        self.elements.append(allPageTable)

    def siteBasedIndicators(self, parameters, ytd_parameters=None):

        reference_parameters = parameters.copy()
        ytd_reference_parameters = ytd_parameters.copy()  

        allPageData = []

        # Row 1
        indicatorTableData = []

        # Days to Permanent Placement
        # Average Days to Move-In
        indicatorNameOriginal = "Average Days to Move-In" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName1, YTDfunctionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName1, functionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow1 = [indicatorName1, functionResult1,YTDfunctionResult1, functionTarget1]

        
        # Median Days to Move-In
        indicatorNameOriginal = "Median Days to Move-In" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()


        catName, indicatorName2, YTDfunctionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName2, functionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow2 = [indicatorName2, functionResult2,YTDfunctionResult2, functionTarget2]

        functionHeaderRow = [catName,"","",""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionFooterRow = [functionFooter]

        functionRow3 = []
        functionRow4 = []
        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2, functionRow3, functionRow4]
        functionTable = Table(tableData, style=rf.TwoIndicatorCondensedTableStyle,
                               colWidths=[1.5*inch, .5*inch, .5*inch, .75*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)


        # Timeliness of Placement
        # % Housed within 30 Days
        indicatorNameOriginal = "% Housed within 30 days" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName1, YTDfunctionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName1, functionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow1 = [indicatorName1, functionResult1, YTDfunctionResult1,functionTarget1]

        
        # % Housed within 60 Days
        indicatorNameOriginal = "% Housed within 60 days" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName2, YTDfunctionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName2, functionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow2 = [indicatorName2, functionResult2,YTDfunctionResult2, functionTarget2]        
        
        # % Housed within 90 Days
        indicatorNameOriginal = "% Housed within 90 days" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName3, YTDfunctionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName3, functionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow3 = [indicatorName3, functionResult3,YTDfunctionResult3, functionTarget3]

        
        # % Housed within 120 Days
        indicatorNameOriginal = "% Housed within 120 days" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName4, YTDfunctionResult4, functionTarget4, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName4, functionResult4, functionTarget4, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow4 = [indicatorName4, functionResult4, YTDfunctionResult4,functionTarget4]

        parameters = reference_parameters.copy()
        total_placed = "{:,}".format(m.movein_php_count(**parameters))

        functionHeaderRow = [catName, "",Paragraph(f"{self.cadence_short_name} Placements: {total_placed}", rf.tableTextStyle),""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionFooterRow = [functionFooter]

        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2, functionRow3, functionRow4]
        functionTable = Table(tableData, style=rf.TwoOrMoreIndicatorCondensedTableStyle,
                               colWidths=[1.4*inch, .5*inch, .5*inch, .75*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)

        row1spacertable = Table([[""]]*len(tableData),style=rf.Row1SpacerCondensedStyle,
                        colWidths=[1.1*inch], rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))

        indicatorTableData.insert(1, row1spacertable)

        indicatorTable = Table([indicatorTableData], style=rf.ProgTypeIndicatorsAlignmentTableStyle)
        allPageData.append([indicatorTable])
        allPageData.append([Spacer(0,inch/10)])
        

        # Row 2
        indicatorTableData = []

        # Housing Retention
        # Housing Retention Rate - 3 Months
        indicatorNameOriginal = "Housing Retention Rate - 3 Months" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName1, YTDfunctionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName1, functionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow1 = [indicatorName1, functionResult1,YTDfunctionResult1, functionTarget1]

        # Housing Retention Rate - 6 Months
        indicatorNameOriginal = "Housing Retention Rate - 6 Months" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName2, YTDfunctionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName2, functionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow2 = [indicatorName2, functionResult2,YTDfunctionResult2, functionTarget2]        
        
        # Housing Retention Rate - 9 Months
        indicatorNameOriginal = "Housing Retention Rate - 9 Months" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName3, YTDfunctionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName3, functionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow3 = [indicatorName3, functionResult3,YTDfunctionResult3, functionTarget3]

        # Housing Retention Rate - 12 Months
        indicatorNameOriginal = "Housing Retention Rate - 12 Months" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName4, YTDfunctionResult4, functionTarget4, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName4, functionResult4, functionTarget4, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow4 = [indicatorName4, functionResult4, YTDfunctionResult4,functionTarget4]

        # Housing Retention Rate - 18 Months
        indicatorNameOriginal = "Housing Retention Rate - 18 Months" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName5, YTDfunctionResult5, functionTarget5, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName5, functionResult5, functionTarget5, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow5 = [indicatorName5, functionResult5, YTDfunctionResult5,functionTarget5]

        # Housing Retention Rate - 24 Months
        indicatorNameOriginal = "Housing Retention Rate - 24 Months" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName6, YTDfunctionResult6, functionTarget6, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName6, functionResult6, functionTarget6, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow6 = [indicatorName6, functionResult6, YTDfunctionResult6,functionTarget6]

        functionHeaderRow = [catName,"","",""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionFooterRow = [functionFooter]

        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2, functionRow3, functionRow4, functionRow5, functionRow6]
        functionTable = Table(tableData, style=rf.TwoOrMoreIndicatorCondensedTableStyle,
                               colWidths=[1.9*inch, .5*inch, .5*inch, .7*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)

        # Income Increases
        # % with Any Income Increase
        indicatorNameOriginal = "% with Any Income Increase" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName1, YTDfunctionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName1, functionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow1 = [indicatorName1, functionResult1,YTDfunctionResult1, functionTarget1]

        # % with Earned Income Increase
        indicatorNameOriginal = "% with Earned Income Increase" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName2, YTDfunctionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName2, functionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow2 = [indicatorName2, functionResult2,YTDfunctionResult2, functionTarget2]

        # % with Benefit and Other Income Increase
        indicatorNameOriginal = "% with Benefit and Other Income Increase" 
   
        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName3, YTDfunctionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName3, functionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow3 = [indicatorName3, functionResult3, YTDfunctionResult3,functionTarget3]
        
        functionHeaderRow = [catName, "","",""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionFooterRow = [functionFooter]

        functionRow4 = []
        functionRow5 = []
        functionRow6 = []
        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2, functionRow3, functionRow4, functionRow5, functionRow6]
        functionTable = Table(tableData, style=rf.TwoIndicatorCondensedTableStyle,
                               colWidths=[2.2*inch, .5*inch, .5*inch, .70*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)

        row1spacertable = Table([[""]]*len(tableData),style=rf.Row1SpacerCondensedStyle,
                        colWidths=[.1*inch], rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))

        #indicatorTableData.insert(1, row1spacertable)

        indicatorTable = Table([indicatorTableData], style=rf.ProgTypeIndicatorsAlignmentTableStyle)
        
        allPageData.append([indicatorTable])
        allPageData.append([Spacer(0,inch/10)])

        # Row 3
        indicatorTableData = []

        # Exit Destinations
        # % of Exits to Permanent Destinations
        indicatorNameOriginal = "% of Exits to Permanent Destinations" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName1, YTDfunctionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName1, functionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow1 = [indicatorName1, functionResult1, YTDfunctionResult1,functionTarget1]

        # % of Exits to Non-Permanent Destinations
        indicatorNameOriginal = "% of Exits to Non-Permanent Destinations" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName2, YTDfunctionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName2, functionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow2 = [indicatorName2, functionResult2, YTDfunctionResult2,functionTarget2]

        # Exits to Homelessness
        indicatorNameOriginal = "% of Exits to Homelessness" 

        catName, indicatorName3, YTDfunctionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName3, functionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow3 = [indicatorName3, functionResult3, YTDfunctionResult3,functionTarget3]

        total_exits = "{:,}".format(m.total_exits(**parameters))   

        functionHeaderRow = [catName, "",Paragraph(f"{self.cadence_short_name} Exits: {total_exits}", rf.tableTextStyle),""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionFooterRow = [functionFooter]    

        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2, functionRow3]
        functionTable = Table(tableData, style=rf.TwoIndicatorCondensedTableStyle,
                               colWidths=[2.5*inch, .5*inch, .5*inch, .75*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)

        row1spacertable = Table([[""]]*len(tableData),style=rf.Row1SpacerCondensedStyle,
                        colWidths=[3.25*inch], rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))

        indicatorTableData.insert(1, row1spacertable)

        indicatorTable = Table([indicatorTableData], style=rf.ProgTypeIndicatorsAlignmentTableStyle)
           
        allPageData.append([indicatorTable])
        allPageTable = Table(allPageData, style=rf.ProgTypeIndicatorsAlignmentTableStyle)  
        self.elements.append(allPageTable)

    def scatteredSiteIndicators(self, parameters, ytd_parameters=None):

        reference_parameters = parameters.copy()
        ytd_reference_parameters = ytd_parameters.copy()  

        allPageData = []

        # Row 1
        indicatorTableData = []

        # Days to Permanent Placement
        # Average Days to Move-In
        indicatorNameOriginal = "Average Days to Move-In" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName1, YTDfunctionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName1, functionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow1 = [indicatorName1, functionResult1,YTDfunctionResult1, functionTarget1]

        # Median Days to Move-In
        indicatorNameOriginal = "Median Days to Move-In" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()


        catName, indicatorName2, YTDfunctionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName2, functionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow2 = [indicatorName2, functionResult2,YTDfunctionResult2, functionTarget2]

        functionHeaderRow = [catName,"","",""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionFooterRow = [functionFooter]

        functionRow3 = []
        functionRow4 = []
        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2, functionRow3, functionRow4]
        functionTable = Table(tableData, style=rf.TwoIndicatorCondensedTableStyle,
                               colWidths=[1.5*inch, .5*inch, .5*inch, .75*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)


        # Timeliness of Placement
        # % Housed within 30 Days
        indicatorNameOriginal = "% Housed within 30 days" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName1, YTDfunctionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName1, functionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow1 = [indicatorName1, functionResult1, YTDfunctionResult1,functionTarget1]

        
        # % Housed within 60 Days
        indicatorNameOriginal = "% Housed within 60 days" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName2, YTDfunctionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName2, functionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow2 = [indicatorName2, functionResult2,YTDfunctionResult2, functionTarget2]        
        
        # % Housed within 90 Days
        indicatorNameOriginal = "% Housed within 90 days" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName3, YTDfunctionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName3, functionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow3 = [indicatorName3, functionResult3,YTDfunctionResult3, functionTarget3]

        
        # % Housed within 120 Days
        indicatorNameOriginal = "% Housed within 120 days" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName4, YTDfunctionResult4, functionTarget4, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName4, functionResult4, functionTarget4, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow4 = [indicatorName4, functionResult4, YTDfunctionResult4,functionTarget4]

        parameters = reference_parameters.copy()
        total_placed = "{:,}".format(m.movein_php_count(**parameters))

        functionHeaderRow = [catName, "",Paragraph(f"{self.cadence_short_name} Placements: {total_placed}", rf.tableTextStyle),""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionFooterRow = [functionFooter]

        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2, functionRow3, functionRow4]
        functionTable = Table(tableData, style=rf.TwoOrMoreIndicatorCondensedTableStyle,
                               colWidths=[1.4*inch, .5*inch, .5*inch, .75*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)

        row1spacertable = Table([[""]]*len(tableData),style=rf.Row1SpacerCondensedStyle,
                        colWidths=[1.1*inch], rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))

        indicatorTableData.insert(1, row1spacertable)

        indicatorTable = Table([indicatorTableData], style=rf.ProgTypeIndicatorsAlignmentTableStyle)
        allPageData.append([indicatorTable])
        allPageData.append([Spacer(0,inch/10)])
        

        # Row 2
        indicatorTableData = []

        # Housing Retention
        # Housing Retention Rate - 3 Months
        indicatorNameOriginal = "Housing Retention Rate - 3 Months" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName1, YTDfunctionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName1, functionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow1 = [indicatorName1, functionResult1,YTDfunctionResult1, functionTarget1]

        # Housing Retention Rate - 6 Months
        indicatorNameOriginal = "Housing Retention Rate - 6 Months" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName2, YTDfunctionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName2, functionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow2 = [indicatorName2, functionResult2,YTDfunctionResult2, functionTarget2]        
        
        # Housing Retention Rate - 9 Months
        indicatorNameOriginal = "Housing Retention Rate - 9 Months" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName3, YTDfunctionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName3, functionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow3 = [indicatorName3, functionResult3,YTDfunctionResult3, functionTarget3]

        # Housing Retention Rate - 12 Months
        indicatorNameOriginal = "Housing Retention Rate - 12 Months" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName4, YTDfunctionResult4, functionTarget4, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName4, functionResult4, functionTarget4, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow4 = [indicatorName4, functionResult4, YTDfunctionResult4,functionTarget4]

        # Housing Retention Rate - 18 Months
        indicatorNameOriginal = "Housing Retention Rate - 18 Months" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName5, YTDfunctionResult5, functionTarget5, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName5, functionResult5, functionTarget5, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow5 = [indicatorName5, functionResult5, YTDfunctionResult5,functionTarget5]

        # Housing Retention Rate - 24 Months
        indicatorNameOriginal = "Housing Retention Rate - 24 Months" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName6, YTDfunctionResult6, functionTarget6, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName6, functionResult6, functionTarget6, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow6 = [indicatorName6, functionResult6, YTDfunctionResult6,functionTarget6]

        functionHeaderRow = [catName,"","",""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionFooterRow = [functionFooter]

        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2, functionRow3, functionRow4, functionRow5, functionRow6]
        functionTable = Table(tableData, style=rf.TwoOrMoreIndicatorCondensedTableStyle,
                               colWidths=[1.9*inch, .5*inch, .5*inch, .7*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)

        # Income Increases
        # % with Any Income Increase
        indicatorNameOriginal = "% with Any Income Increase" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName1, YTDfunctionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName1, functionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow1 = [indicatorName1, functionResult1,YTDfunctionResult1, functionTarget1]

        # % with Earned Income Increase
        indicatorNameOriginal = "% with Earned Income Increase" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName2, YTDfunctionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName2, functionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow2 = [indicatorName2, functionResult2,YTDfunctionResult2, functionTarget2]

        # % with Benefit and Other Income Increase
        indicatorNameOriginal = "% with Benefit and Other Income Increase" 
   
        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName3, YTDfunctionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName3, functionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow3 = [indicatorName3, functionResult3, YTDfunctionResult3,functionTarget3]
        
        functionHeaderRow = [catName, "","",""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionFooterRow = [functionFooter]

        functionRow4 = []
        functionRow5 = []
        functionRow6 = []
        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2, functionRow3, functionRow4, functionRow5, functionRow6]
        functionTable = Table(tableData, style=rf.TwoIndicatorCondensedTableStyle,
                               colWidths=[2.2*inch, .5*inch, .5*inch, .70*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)

        row1spacertable = Table([[""]]*len(tableData),style=rf.Row1SpacerCondensedStyle,
                        colWidths=[.1*inch], rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))

        #indicatorTableData.insert(1, row1spacertable)

        indicatorTable = Table([indicatorTableData], style=rf.ProgTypeIndicatorsAlignmentTableStyle)
        
        allPageData.append([indicatorTable])
        allPageData.append([Spacer(0,inch/10)])

        # Row 3
        indicatorTableData = []

        # Exit Destinations
        # % of Exits to Permanent Destinations
        indicatorNameOriginal = "% of Exits to Permanent Destinations" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName1, YTDfunctionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName1, functionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow1 = [indicatorName1, functionResult1, YTDfunctionResult1,functionTarget1]

        # % of Exits to Non-Permanent Destinations
        indicatorNameOriginal = "% of Exits to Non-Permanent Destinations" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName2, YTDfunctionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName2, functionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow2 = [indicatorName2, functionResult2, YTDfunctionResult2,functionTarget2]

        # Exits to Homelessness
        indicatorNameOriginal = "% of Exits to Homelessness" 

        catName, indicatorName3, YTDfunctionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName3, functionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow3 = [indicatorName3, functionResult3, YTDfunctionResult3,functionTarget3]

        total_exits = "{:,}".format(m.total_exits(**parameters))   

        functionHeaderRow = [catName, "",Paragraph(f"{self.cadence_short_name} Exits: {total_exits}", rf.tableTextStyle),""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionFooterRow = [functionFooter]    

        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2, functionRow3]
        functionTable = Table(tableData, style=rf.TwoIndicatorCondensedTableStyle,
                               colWidths=[2.5*inch, .5*inch, .5*inch, .75*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)

        row1spacertable = Table([[""]]*len(tableData),style=rf.Row1SpacerCondensedStyle,
                        colWidths=[3.25*inch], rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))

        indicatorTableData.insert(1, row1spacertable)

        indicatorTable = Table([indicatorTableData], style=rf.ProgTypeIndicatorsAlignmentTableStyle)
           
        allPageData.append([indicatorTable])
        allPageTable = Table(allPageData, style=rf.ProgTypeIndicatorsAlignmentTableStyle)  
        self.elements.append(allPageTable)

    def preventionIndicators(self, parameters, ytd_parameters=None):
        allPageData = [] 
 
        reference_parameters = parameters.copy()
        ytd_reference_parameters = ytd_parameters.copy()  
        

        # Row 1
        indicatorTableData = []

        # Days to Permanent Destination
        # Average Days to Permanent Destination
        indicatorNameOriginal = "Average Days to Permanent Destination" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName1, YTDfunctionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName1, functionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow1 = [indicatorName1, functionResult1, YTDfunctionResult1,functionTarget1]

        
        # Median Days to Permanent Placement
        indicatorNameOriginal = "Median Days to Permanent Destination" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()


        catName, indicatorName2, YTDfunctionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName2, functionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow2 = [indicatorName2, functionResult2, YTDfunctionResult2,functionTarget2]

        functionHeaderRow = [catName,"","",""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionFooterRow = [functionFooter]

        functionRow3 = []
        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2, functionRow3]
        functionTable = Table(tableData, style=rf.TwoOrMoreIndicatorCondensedTableStyle,
                               colWidths=[2.1*inch, .45*inch, .45*inch, .7*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)
        
        # Exit Destinations
        # % of Exits to Permanent Destinations
        indicatorNameOriginal = "% of Exits to Permanent Destinations" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName1, YTDfunctionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName1, functionResult1, functionTarget1, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow1 = [indicatorName1, functionResult1,YTDfunctionResult1, functionTarget1]

        # % of Exits to Non-Permanent Destinations
        indicatorNameOriginal = "% of Exits to Non-Permanent Destinations" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName2, YTDfunctionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName2, functionResult2, functionTarget2, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow2 = [indicatorName2, functionResult2,YTDfunctionResult2, functionTarget2]

        # Exits to Homelessness
        indicatorNameOriginal = "% of Exits to Homelessness" 

        parameters = reference_parameters.copy()
        ytd_parameters = ytd_reference_parameters.copy()

        catName, indicatorName3, YTDfunctionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, ytd_parameters)
        catName, indicatorName3, functionResult3, functionTarget3, functionFooter = self.returnFormattedFunctionData(indicatorNameOriginal, parameters)
        functionRow3 = [indicatorName3, functionResult3, YTDfunctionResult3, functionTarget3]

        total_exits = "{:,}".format(m.total_exits(**parameters))

        functionHeaderRow = [catName, "",Paragraph(f"{self.cadence_short_name} Exits: {total_exits}", rf.tableTextStyle),""]
        functionTitleRow = ["",Paragraph(f"{self.cadence_short_name}",rf.tableSecondaryHeader),Paragraph("YTD",rf.tableSecondaryHeader), Paragraph("Agency Goal", rf.tableSecondaryHeader)]
        functionFooterRow = [functionFooter]    

        tableData = [functionHeaderRow, functionTitleRow, functionRow1, functionRow2, functionRow3]
        functionTable = Table(tableData, style=rf.TwoIndicatorCondensedTableStyle,
                               colWidths=[2.2*inch, .45*inch, .45*inch, .7*inch],
                               rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))
        
        indicatorTableData.append(functionTable)

        row1spacertable = Table([[""]]*len(tableData),style=rf.Row1SpacerCondensedStyle,
                        colWidths=[.1*inch], rowHeights=rf.ProgTypeIndicatorsRowHeights(len(tableData)))

        #indicatorTableData.insert(1, row1spacertable)

        indicatorTable = Table([indicatorTableData], style=rf.ProgTypeIndicatorsAlignmentTableStyle)
        self.elements.append(indicatorTable)

    def careCoordIndicators(self, parameters, ytd_parameters=None):
        reference_parameters = parameters.copy()
        ytd_reference_parameters = ytd_parameters.copy()  
        
       
        self.elements.append(Paragraph("Coming Soon!", rf.tableHeaderStyle))
        self.elements.append(Spacer(10, inch/6))

    def employmentIndicators(self, parameters, ytd_parameters=None):
        reference_parameters = parameters.copy()
        ytd_reference_parameters = ytd_parameters.copy()  
        
        
        self.elements.append(Paragraph("Coming Soon!", rf.tableHeaderStyle))
        self.elements.append(Spacer(10, inch/6))
        
    def housingPartnershipsIndicators(self, parameters, ytd_parameters=None):
        reference_parameters = parameters.copy()
        ytd_reference_parameters = ytd_parameters.copy()  
        
       
        self.elements.append(Paragraph("Coming Soon!", rf.tableHeaderStyle))
        self.elements.append(Spacer(10, inch/6))       

    def behavioralHealthIndicators(self, parameters, ytd_parameters=None):
        reference_parameters = parameters.copy()
        ytd_reference_parameters = ytd_parameters.copy()  
        
        
        self.elements.append(Paragraph("Coming Soon!", rf.tableHeaderStyle))
        self.elements.append(Spacer(10, inch/6))       

    def accessCenterIndicators(self, parameters, ytd_parameters=None):
        reference_parameters = parameters.copy()
        ytd_reference_parameters = ytd_parameters.copy()  
        

        self.elements.append(Paragraph("Coming Soon!", rf.tableHeaderStyle))
        self.elements.append(Spacer(10, inch/6))
        
    def returnFormattedFunctionData(self, indicator_name, parameters):
        function_parameters = parameters
        
        for ProgramType, IndicatorCategoryDict in self.agency_indicator_functions_dict.items():
            for IndicatorCategoryName, IndicatorNameDict in IndicatorCategoryDict.items():
                for IndicatorName, IndicatorDetailsDict in IndicatorNameDict.items():
                    if indicator_name == IndicatorName and parameters['program_type'][0] == ProgramType:
                        parameters_update = IndicatorDetailsDict['IndicatorParameter']
                        function_parameters.update(parameters_update)
                        result = IndicatorDetailsDict['IndicatorFunction'](**function_parameters)
                        functionTarget = IndicatorDetailsDict['Format'].format(IndicatorDetailsDict['Target'])
                        
                        if isinstance(result, numbers.Number):
                            if IndicatorDetailsDict['IndicatorType'] == 'larger':
                                if result < IndicatorDetailsDict['Target']:
                                    
                                    functionResult = Paragraph(IndicatorDetailsDict['Format'].format(result), rf.tableValuesMissStyle)
                                else:
                                    functionResult = Paragraph(IndicatorDetailsDict['Format'].format(result), rf.tableValuesExceedStyle)
                            else:
                                if result >= IndicatorDetailsDict['Target']:
                                    functionResult = Paragraph(IndicatorDetailsDict['Format'].format(result), rf.tableValuesMissStyle)
                                else:
                                    functionResult = Paragraph(IndicatorDetailsDict['Format'].format(result), rf.tableValuesExceedStyle)
                        else:
                            ##### LIST OF MISSING FUNCTIONS #####
                            missing_functions = ['Utilization Rate','% Document Ready','% with HSP within 30 Days']
                            if any(mis_funct in indicator_name for mis_funct in missing_functions):
                                functionResult = Paragraph("Coming<br/>Soon", rf.ComingSoonStyle)
                            else:
                                functionResult = Paragraph("-", rf.tableValuesStyle)                            
                        return Paragraph(IndicatorCategoryName, rf.tableHeaderStyle), Paragraph(indicator_name, rf.tableTextStyle), functionResult, Paragraph(functionTarget, rf.tableTargetStyle), Paragraph(IndicatorDetailsDict['IndicatorFooter'], rf.tableFooterStyle)

    def returnProgTypes(self, program_id=None, region=None, department=None):
        progtype_list = [ProgramType for ProgramType in self.agency_indicator_functions_dict.keys()]

        if program_id:
            progtype_list = [ProgType for DeptDict in self.master_dict.values()
                    for ProgTypeDict in DeptDict.values()
                    for ProgType, ProgDict in ProgTypeDict.items()
                    for MergedIDDict in ProgDict.values()
                    if program_id in MergedIDDict]

        elif region:
            progtype_set = set()
            for Region, DeptDict in self.master_dict.items():
                for ProgTypeDict in DeptDict.values():
                    for ProgType in ProgTypeDict.keys():
                        if region in Region:
                            progtype_set.add(ProgType)
            
            progtype_list = [x for x in progtype_list if x in progtype_set]

        elif department:
            new_progtype_list = [ProgType for Region, DeptDict in self.master_dict.items()
                                for Dept, ProgTypeDict in DeptDict.items()
                                for ProgType, ProgDict in ProgTypeDict.items()
                                if department in Dept]
                            
            progtype_list = [x for x in progtype_list if x in new_progtype_list]

        return progtype_list

    def pullInidcatorDict(self, grant_code):
        
        for DeptDict in self.kpi_dict.values():
            for ProgTypeDict in DeptDict.values():
                for GrantCodeDict in ProgTypeDict.values():
                    for GrantCode, IndicatorDict in GrantCodeDict.items():
                        if grant_code in GrantCode:
                            return IndicatorDict               
def sharepoint_upload(cadence_long_name,fy_name):
    CLIENT_ID = '#########'
    CLIENT_SECRET = '#########'
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
        drive_id = "b!AlQvkpfM-Um7b88PHosfeyAbLTbrddlNqTcLh2MekuK7BLLnlvscQJ_Xokn5c40j" ## this goes to document library within QA site
        
        # Specify the folder ID where you want to create the new folder
        parent_folder_id = "01YEVPESGMWGEGXEYTYRBIQCAF2UACMLD4"  # goes to the FY25 folder in Data and Eval/ Working Files/ Quarterly and Monthly Reports

        # Get the folder name from user input
        folder_name = cadence_long_name

        url = f"{GRAPH_API_ENDPOINT}/drives/{drive_id}/items/{parent_folder_id}/children"

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        # Define folder metadata
        data = {
            "name": folder_name,
            "folder": {},
            "@microsoft.graph.conflictBehavior": "rename"
        }

        # Make the POST request to create the folder
        response = requests.post(url, headers=headers, json=data)

        if response.status_code == 201:
            print(f"Folder '{folder_name}' created successfully in the specified folder!")

            # Navigate into the new folder to upload files
            new_folder_id = response.json()['id']
            print(f"New folder ID: {new_folder_id}")

            # Prompt for fiscal year and month
            fiscal_year = fy_name
            month = cadence_long_name

            # Define the base location and append the fiscal year
            base_location = "/home/pathdb/db_helper_functions/completed_reports"
            location = os.path.join(base_location, fiscal_year, month)
            if os.path.exists(location):
                print(f"Items in {location}:")
                
                # List all items (files and folders) in the specified location
                for item in os.listdir(location):
                    print(item)  # Print both files and directories
                    
                    # Upload each file
                    file_path = os.path.join(location, item)
                    if os.path.isfile(file_path):
                        with open(file_path, 'rb') as file:
                            upload_url = f"{GRAPH_API_ENDPOINT}/drives/{drive_id}/items/{new_folder_id}:/{item}:/content"
                            upload_response = requests.put(upload_url, headers=headers, data=file)
                            
                            if upload_response.status_code in [200, 201]:
                                print(f"Uploaded '{item}' successfully.")
                            else:
                                print(f"Failed to upload '{item}': {upload_response.status_code}, {upload_response.text}")
            else:
                print(f"The path {location} does not exist.")
        else:
            print(f"Error: {response.status_code}, {response.text}")
def get_reports():
    report_type = None
    report_cadence = None
    master_dict = m.all_programs_dict()

    global report_name
    while report_cadence not in ['Monthly','Quarterly']:
        report_cadence_input = input("Enter report cadence (default: Monthly)\n\tM for Montly\n\tQ for Quarterly: ").lower() or 'm'

        current_date = date.today()
        fiscal_year = current_date.year + 1 if current_date.month > 6 else current_date.year

        fiscal_year_input = input(f"Enter the fiscal year (default: {fiscal_year}):  ") or fiscal_year
        fiscal_year = int(fiscal_year_input)

        fy_start_date = date(fiscal_year - 1, 7, 1)
        fy_name = 'FY' + str(fiscal_year)[2:]
        
        if report_cadence_input == 'm':
            report_cadence = 'Monthly'

            # Default month is the previous full month
            default_month = current_date.month - 1 if current_date.month > 1 else 12

            month_input = input(f"Enter the month number (default: {default_month}):  ") or default_month
            month = int(month_input)

            if month == 1:
                start_date = date(fiscal_year, 1, 1)
                end_date = date(fiscal_year, 1, 31)
            elif month == 2:
                start_date = date(fiscal_year, 2, 1)
                end_date = date(fiscal_year, 2, 29) if fiscal_year % 4 == 0 else date(fiscal_year, 2, 28)
            elif month == 3:
                start_date = date(fiscal_year, 3, 1)
                end_date = date(fiscal_year, 3, 31)
            elif month == 4:
                start_date = date(fiscal_year, 4, 1)
                end_date = date(fiscal_year, 4, 30)
            elif month == 5:
                start_date = date(fiscal_year, 5, 1)
                end_date = date(fiscal_year, 5, 31)
            elif month == 6:
                start_date = date(fiscal_year, 6, 1)
                end_date = date(fiscal_year, 6, 30)
            elif month == 7:
                start_date = date(fiscal_year - 1, 7, 1)
                end_date = date(fiscal_year - 1, 7, 31)
            elif month == 8:
                start_date = date(fiscal_year - 1, 8, 1)
                end_date = date(fiscal_year - 1, 8, 31)
            elif month == 9:
                start_date = date(fiscal_year - 1, 9, 1)
                end_date = date(fiscal_year - 1, 9, 30)
            elif month == 10:
                start_date = date(fiscal_year - 1, 10, 1)
                end_date = date(fiscal_year - 1, 10, 31)
            elif month == 11:
                start_date = date(fiscal_year - 1, 11, 1)
                end_date = date(fiscal_year - 1, 11, 30)
            elif month == 12:
                start_date = date(fiscal_year - 1, 12, 1)
                end_date = date(fiscal_year - 1, 12, 31)

            cadence_name = calendar.month_name[month]

            report_type = 'Department'
            department_names = {
                'GLA-SPA5': 'Greater LA (SPA 5)',
                'GLA-SPA7': 'Greater LA (SPA 7)',
                'GLA-SPA8': 'Greater LA (SPA 8)',
                'GLA-SSVF': 'Greater LA (SSVF)',
                'GLA-Employment': 'Greater LA (Employment)',
                'Health': 'Health Services',
                'MLA-Fam': 'Metro LA (Families)',
                'MLA-Indv': 'Metro LA (Individuals)',
                'MLA-Transit': 'Metro LA (Metro Transit)',
                'Interim-GLA': 'Interim Housing LA (Greater LA)',
                'Interim-SPA4': 'Interim Housing LA (SPA 4)',
                'PSS-DTLA': 'Permanent Supportive Services (DTLA Place Based)',
                'PSS-NonDTLA': 'Permanent Supportive Services (Non DTLA Place Based)',
                'PSS-Scattered': 'Permanent Supportive Services (Scattered)',
                'SD': 'San Diego',
                'SB': 'Santa Barbara',
                'Santa Clara': 'Santa Clara',
                'Orange County': 'Orange County',
            }

            full_dept = None
            while full_dept not in department_names.values():
                dept_input = input(f"\nSelect Dept/Region by Short Name ({'/'.join(department_names.keys())})\n\tHit enter to create all Dept/Region reports.").upper()
                full_dept = department_names.get(dept_input)

                if full_dept:
                    print(f"Creating {full_dept} Report")
                    QuarterlyReports("Monthly", full_dept, fy_name, cadence_name, start_date, end_date, fy_start_date)
                    sharepoint_upload(cadence_name,fy_name)
                elif not dept_input:
                    print("Creating All Department Reports")
                    master_dict = m.all_programs_dict()
                    for Region, DeptDict in master_dict.items():
                        for Dept, ProgTypeDict in DeptDict.items():
                            QuarterlyReports("Monthly", Dept, fy_name, cadence_name, start_date, end_date, fy_start_date)
                    sharepoint_upload(cadence_name,fy_name)
                    QuarterlyReports("Monthly", "Los Angeles County", fy_name, cadence_name, start_date, end_date, fy_start_date)

                    break 
                else:
                    print("\nInvalid input!")
                    
        elif report_cadence_input == 'q':
            report_cadence = 'Quarterly'

            # Default quarter is the previous full quarter
            default_quarter = 1
            if current_date.month < 4:
                default_quarter = 2
            elif current_date.month < 7:
                default_quarter = 3
            elif current_date.month < 10:
                default_quarter = 4
            
            quarter_input = input(f"Enter the quarter number (default: Q{default_quarter}):  ") or default_quarter
            quarter = int(quarter_input)

            if quarter == 1:
                start_date = date(fiscal_year - 1, 7, 1)
                end_date = date(fiscal_year - 1, 9, 30)
            elif quarter == 2:
                start_date = date(fiscal_year - 1, 10, 1)
                end_date = date(fiscal_year - 1, 12, 31)
            elif quarter == 3:
                start_date = date(fiscal_year, 1, 1)
                end_date = date(fiscal_year, 3, 31)
            else:
                start_date = date(fiscal_year, 4, 1)
                end_date = date(fiscal_year, 6, 30)

            cadence_name = 'Q' + str(quarter)

            while report_type not in ['Executive Summary', 'Region', 'Department']:
                report_type_input = input("Enter report type (default: Executive Summary)\n\tE for Executive Summary\n\tR for Region\n\tD for Department\n\tA for All Reports: ").lower() or 'e'
                if report_type_input == 'e' or not report_type_input:
                    report_type = 'Executive Summary'
                    
                    QuarterlyReports("Executive Summary", "Executive Summary", fy_name, cadence_name, start_date, end_date, fy_start_date)
                elif report_type_input == 'r':
                    report_type = 'Region'
                    
                    region_names = {
                        'LA': 'Los Angeles County',
                        'SD': 'San Diego County',
                        'SB': 'Santa Barbara County',
                        'SCC': 'Santa Clara County',
                        'OC': 'Orange County'
                    }
                    
                    full_region = None
                    while full_region not in region_names.values():
                        region_input = input(f"\nSelect Region by Short Name ({'/'.join(region_names.keys())}) (default: LA): ").upper() or "LA"
                        full_region = region_names.get(region_input)
                
                        if full_region:
                            print(f"Creating {full_region} Report")

                            QuarterlyReports("Region", full_region, fy_name, cadence_name, start_date, end_date, fy_start_date)
                            sharepoint_upload(cadence_name,fy_name)
                        elif not region_input:
                            print("Creating Los Angeles County Report")
                            full_region = 'Los Angeles County'
                            QuarterlyReports("Region", full_region, fy_name, cadence_name, start_date, end_date, fy_start_date)
                        else:
                            print("\nInvalid input!")

                elif report_type_input == 'd':
                    report_type = 'Department'
                    department_names = {
                        'GLA-SPA5': 'Greater LA (SPA 5)',
                        'GLA-SPA7': 'Greater LA (SPA 7)',
                        'GLA-SPA8': 'Greater LA (SPA 8)',
                        'GLA-SSVF': 'Greater LA (SSVF)',
                        'GLA-Employment': 'Greater LA (Employment)',
                        'Health': 'Health Services',
                        'MLA-Fam': 'Metro LA (Families)',
                        'MLA-Indv': 'Metro LA (Individuals)',
                        'MLA-Transit': 'Metro LA (Metro Transit)',
                        'Interim-GLA': 'Interim Housing LA (Greater LA)',
                        'Interim-SPA4': 'Interim Housing LA (SPA 4)',
                        'PSS-DTLA': 'Permanent Supportive Services (DTLA Place Based)',
                        'PSS-NonDTLA': 'Permanent Supportive Services (Non DTLA Place Based)',
                        'PSS-Scattered': 'Permanent Supportive Services (Scattered)',
                        'SD': 'San Diego',
                        'SB': 'Santa Barbara',
                        'Santa Clara': 'Santa Clara',
                        'Orange County': 'Orange County',
                    }

                    full_dept = None
                    while full_dept not in department_names.values():
                        dept_input = input(f"\nSelect Dept by Short Name ({'/'.join(department_names.keys())})\n\tHit enter to create all department reports.").upper()
                        full_dept = department_names.get(dept_input)

                        if full_dept:
                            print(f"Creating {full_dept} Report")

                            QuarterlyReports("Department", full_dept, fy_name, cadence_name, start_date, end_date, fy_start_date)
                            sharepoint_upload(cadence_name,fy_name)
                        elif not dept_input:
                            print("Creating All Department Reports")
                            for Region, DeptDict in master_dict.items():
                                for Dept, ProgTypeDict in DeptDict.items():

                                    QuarterlyReports("Department", Dept, fy_name, cadence_name, start_date, end_date, fy_start_date)
                            sharepoint_upload(cadence_name,fy_name)
                            break
                        else:
                            print("\nInvalid input!")
                elif report_type_input == 'a':
                    
                    QuarterlyReports("Executive Summary", "Executive Summary", fy_name, cadence_name, start_date, end_date, fy_start_date)
                    for Region, DeptDict in master_dict.items():
                        for Dept, ProgTypeDict in DeptDict.items():
                            QuarterlyReports("Department", Dept, fy_name, cadence_name, start_date, end_date, fy_start_date)
                    QuarterlyReports("Region", "Los Angeles County",fy_name, cadence_name, start_date, end_date, fy_start_date)
                    break
                else:
                    print("\nInvalid input!\n")

        else:
            print("\nInvalid input!\n")


if __name__ == '__main__':
    
   get_reports()
