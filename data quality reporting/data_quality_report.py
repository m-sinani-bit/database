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
from matplotlib.ticker import PercentFormatter
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

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
from reportlab.lib.colors import Color, gray, black, green, blue,orange, red, purple, lavender
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics
from reportlab.platypus.tableofcontents import TableOfContents
from reportlab.lib.colors import HexColor

import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter
from PIL import Image as IMG
import numpy as np

import dq_functions_server as dq
import measure_definitions as m
import quality_report_format_server as rf
import db_setup_functions as dsf


database_name = "PATHParticipantDB"
indicator_csv = "PATHIndicators.csv"
kpi_log_csv = "kpilog.csv"
glossary_csv = "DQ glossary.csv"

report_folder = "dq_completed_reports"

image_library_name = 'images'
chart_library_name = 'charts'
font_library_name = 'fonts'

image_directory = "images"
chart_directory = "charts"
font_directory = "fonts"

PATHLightBlue = (173/255, 216/255, 230/255) 
PATHLightPurple = (220/255, 190/255, 221/255)  
PATHLightGreen = (188/255, 232/255, 201/255)    
PATHBlue = (0.0/255, 174.0/255, 239.0/255)
PATHPurple =(138.0/255, 40.0/255, 143.0/255)
PATHGreen = (34.0/255,178/255,76/255)


image_directory = os.path.join(os.getcwd(), image_library_name)
chart_directory = os.path.join(os.getcwd(), chart_library_name)
font_directory = os.path.join(os.getcwd(), font_library_name)


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
        elif report_type == "Quarterly":
            print(f"Beginning Quarterly Report for {self.division_name}")
            self.quarterlyReport(self.division_name)
        else:
            print(f"Beginning Monthly Report for {self.division_name}")
            self.monthlyReport(self.division_name)
        
        global report_name
        report_name = fy_name + " " + cadence_name + " Data Quality Report"
        Path(os.path.join(os.getcwd(),report_folder, fy_name, cadence_name)).mkdir(parents=True, exist_ok=True)
        
        modified_path = report_name + " - " + self.division_name + ".pdf"
        self.doc = ReportDocTemplate(os.path.join(os.getcwd(),report_folder, fy_name, cadence_name, modified_path), pagesize=LETTER, topMargin=inch/2, leftMargin=inch/2, rightMargin=inch/2, bottomMargin=inch/2)
        self.doc.multiBuild(self.elements, canvasmaker=ReportCanvas)

    def glossary(self):    
        print(f"Starting Glossary")

        self.elements.append(Paragraph("Glossary", rf.pageHeaderStyle))
        self.elements.append(Spacer(0, inch/6))


        for CategoryName, CategoryDict in self.glossary_dict.items():
            self.elements.append(Paragraph(CategoryName,rf.tableHeaderStyle))
            self.elements.append(Spacer(0, inch/15))

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

        
        
    def monthlyReport(self, division):
        self.monthlyTitlePage()
        #self.tableOfContents()
        self.glossary()
        
        
        if division == 'Los Angeles County':
            for Region, DeptDict in self.master_dict.items():
                if division == Region:
                    self.division1(region=Region) 
                    for Dept, ProgTypeDict in DeptDict.items():
                        self.division1(department=Dept) 
                        self.programPagesTitlePage(Dept)
                        for ProgType, ProgDict in ProgTypeDict.items():
                            for Prog, MergedIDDict in ProgDict.items():
                                    self.programPage(ProgType, Prog, MergedIDDict)
        elif division == 'Executive':
            self.executive_division1()
            self.departmentPagesTitlePage()
            for Region, DeptDict in self.master_dict.items():
                for Dept, ProgTypeDict in DeptDict.items():
                        self.department_division1(department=Dept) 

        
        else:
            for Region, DeptDict in self.master_dict.items():
                for Dept, ProgTypeDict in DeptDict.items():
                    if Dept==division:
                        self.division1(department=Dept) 
                        self.programPagesTitlePage(division)
                        for ProgType, ProgDict in ProgTypeDict.items():
                            for Prog, MergedIDDict in ProgDict.items():
                                    self.programPage(Dept,ProgType, Prog, MergedIDDict)
    def quarterlyReport(self, division):
        self.quarterlyTitlePage()
        #self.tableOfContents()
        self.glossary()
        
        
        if division == 'Los Angeles County':
            for Region, DeptDict in self.master_dict.items():
                if division == Region:
                    self.division1(region=Region) 
                    for Dept, ProgTypeDict in DeptDict.items():
                        self.division1(department=Dept) 
                        self.programPagesTitlePage(Dept)
                        for ProgType, ProgDict in ProgTypeDict.items():
                            for Prog, MergedIDDict in ProgDict.items():
                                    self.programPage(ProgType, Prog, MergedIDDict)
        
        elif division =='Executive':
            self.executive_division1()
            self.departmentPagesTitlePage()
            for Region, DeptDict in self.master_dict.items():
                for Dept, ProgTypeDict in DeptDict.items():
                        self.department_division1(department=Dept) 
            
        else:
            for Region, DeptDict in self.master_dict.items():
                for Dept, ProgTypeDict in DeptDict.items():
                    if Dept==division:
                        self.division1(department=Dept) 
                        self.programPagesTitlePage(division)
                        for ProgType, ProgDict in ProgTypeDict.items():
                            for Prog, MergedIDDict in ProgDict.items():
                                    self.programPage(Dept,ProgType, Prog, MergedIDDict)
                                        
    def quarterlyTitlePage(self):
        drawing = Drawing(width=inch*4.75, height=inch*6)
        circle = Circle(inch*2.5, inch*1.9, inch*1.3)
        circle.fillColor = rf.PATHLightBlue
        circle.strokeColor = rf.PATHLightBlue
        circle.strokeWidth = 0
        drawing.add(circle)
        
        circle = Circle(inch*.75,inch*4,inch*1.5)
        circle.fillColor = rf.PATHLightRed
        circle.strokeColor = rf.PATHLightRed
        circle.strokeWidth = 0
        drawing.add(circle)

        circle = Circle(inch*3,inch*6.5,inch*1.9)
        circle.fillColor = rf.PATHLightGreen
        circle.strokeColor = rf.PATHLightGreen
        circle.strokeWidth = 0
        drawing.add(circle)

        circle = Circle(inch*5,inch*.1,inch*2)
        circle.fillColor = rf.PATHLightPurple
        circle.strokeColor = rf.PATHLightPurple
        circle.strokeWidth = 0
        drawing.add(circle)

        drawing.hAlign = 'CENTER'

        titleLabel = Label()
        titleLabel.setText(self.fy_name)
        titleLabel.fontSize = 150
        titleLabel.fontName = 'MontserratSemiBold'
        titleLabel.dy = 5*inch
        titleLabel.dx = 1.25*inch
        drawing.add(titleLabel)
        
        titleLabel = Label()
        titleLabel.setText(self.cadence_short_name)
        titleLabel.fontSize = 150
        titleLabel.fontName = 'Montserrat'
        titleLabel.dy = 3.25*inch
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

        titleLabelText = Paragraph("Data Quality Report",rf.titlePageStyle)
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
        circle = Circle(inch*2.5, inch*1.9, inch*1.3)
        circle.fillColor = rf.PATHLightBlue
        circle.strokeColor = rf.PATHLightBlue
        circle.strokeWidth = 0
        drawing.add(circle)
        
        circle = Circle(inch*.75,inch*4,inch*1.5)
        circle.fillColor = rf.PATHLightRed
        circle.strokeColor = rf.PATHLightRed
        circle.strokeWidth = 0
        drawing.add(circle)

        circle = Circle(inch*3,inch*6.5,inch*1.9)
        circle.fillColor = rf.PATHLightGreen
        circle.strokeColor = rf.PATHLightGreen
        circle.strokeWidth = 0
        drawing.add(circle)

        circle = Circle(inch*5,inch*.1,inch*2)
        circle.fillColor = rf.PATHLightPurple
        circle.strokeColor = rf.PATHLightPurple
        circle.strokeWidth = 0
        drawing.add(circle)
        


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

        titleLabelText = Paragraph("Data Quality Report",rf.titlePageStyle)
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
        circle = Circle(inch*2.5, inch*1.9, inch*1.3)
        circle.fillColor = rf.PATHLightBlue
        circle.strokeColor = rf.PATHLightBlue
        circle.strokeWidth = 0
        drawing.add(circle)
        
        circle = Circle(inch*.75,inch*4,inch*1.5)
        circle.fillColor = rf.PATHLightRed
        circle.strokeColor = rf.PATHLightRed
        circle.strokeWidth = 0
        drawing.add(circle)

        circle = Circle(inch*3,inch*6.5,inch*1.9)
        circle.fillColor = rf.PATHLightGreen
        circle.strokeColor = rf.PATHLightGreen
        circle.strokeWidth = 0
        drawing.add(circle)

        circle = Circle(inch*5,inch*.1,inch*2)
        circle.fillColor = rf.PATHLightPurple
        circle.strokeColor = rf.PATHLightPurple
        circle.strokeWidth = 0
        drawing.add(circle)
        

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
        
    def departmentPagesTitlePage(self):
        drawing = Drawing(width=inch*4.75, height=inch*7)
        circle = Circle(inch*2.5, inch*1.9, inch*1.3)
        circle.fillColor = rf.PATHLightBlue
        circle.strokeColor = rf.PATHLightBlue
        circle.strokeWidth = 0
        drawing.add(circle)
        
        circle = Circle(inch*.75,inch*4,inch*1.5)
        circle.fillColor = rf.PATHLightRed
        circle.strokeColor = rf.PATHLightRed
        circle.strokeWidth = 0
        drawing.add(circle)

        circle = Circle(inch*3,inch*6.5,inch*1.9)
        circle.fillColor = rf.PATHLightGreen
        circle.strokeColor = rf.PATHLightGreen
        circle.strokeWidth = 0
        drawing.add(circle)

        circle = Circle(inch*5,inch*.1,inch*2)
        circle.fillColor = rf.PATHLightPurple
        circle.strokeColor = rf.PATHLightPurple
        circle.strokeWidth = 0
        drawing.add(circle)
        

        drawing.hAlign = 'CENTER'

        self.elements.append(drawing)

        titleLabelText = Paragraph("Department Summaries",rf.programsummarytitlePageStyle)
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
        self.elements.append(Paragraph("Agency Indicators by Department", rf.pageHeaderStyle))
        self.elements.append(Spacer(0, inch/4))
        
        
        self.elements.append(PageBreak())

        # Program Type Indicators
        self.elements.append(Paragraph("Program Type Indicators, All Agency", rf.pageHeaderStyle))
        self.elements.append(Spacer(0, inch/4))


        self.elements.append(PageBreak())

        # Agency Demographics
        self.elements.append(Paragraph("All Agency Demographics", rf.pageHeaderStyle))   
        self.elements.append(Spacer(0, inch/10))
        self.elements.append(Paragraph("Fiscal Year-to-Date", rf.subSectionHeaderStyle))
        self.elements.append(Spacer(0, inch/4))

        self.elements.append(PageBreak())

    def linecharts(self,function,filename):
        department_colors = {
        'San Diego': 'red',
        'Santa Barbara': 'darkblue',
        'Orange County': 'green',
        'Santa Clara': 'orange',
        'Families': 'purple',
        'Metro LA': 'brown',
        'Permanent Supportive Services': 'cornflowerblue',
        'South County': 'teal',
        'Veterans': 'magenta',
        'West LA': 'gray',
        }

        days_of_interest = [1, 4, 7, 11, 14]


        # Collect results for the specific days
        results_by_department = {d: [] for d in department_colors}
        agency_results = []

        for days in days_of_interest:
            parameters = ({"start_date": self.start_date, "end_date": self.end_date,"days":days})

            agency_number =function( **parameters)
            agency_results.append(agency_number)

            for d,color in department_colors.items():
                parameters["department"] = [d]
                number = function(**parameters)
                results_by_department[d].append(number)
        #print(results_by_department)
        #print(agency_results)
        # Create the line plot
        width = 3.625
        height = 2.167
        image_dpi = 600
        fig, ax = plt.subplots(figsize=(width, height), dpi=image_dpi)

        for d, values in results_by_department.items():
            ax.plot(days_of_interest, values, linestyle='-', label=f'{d}', linewidth=0.5, markersize=2, color=department_colors[d])

        ax.plot(days_of_interest, agency_results, marker='o', linestyle='-', label='Agency', linewidth=1, markersize=2, color='black')
        ax.axhline(0, color='white', linestyle='--')

        # Set x-axis tick labels with font size
        ax.set_xticks(days_of_interest)
        ax.set_xticklabels([f'Day {days}' for days in days_of_interest], fontsize=4, ha='center')

        ax.yaxis.set_major_formatter(PercentFormatter(100))
        ax.set_yticklabels([f'{tick * 100:.1f}%' for tick in ax.get_yticks()], fontsize=4)

        # Set y-axis limit to start from zero
        #ax.set_ylim(bottom=0, top=1)

        ax.legend(loc='upper left', bbox_to_anchor=(-0.025,-0.1), fontsize=4, ncol=4)

        # Save the plot as an image
        plt.savefig(filename, bbox_inches='tight')
        plt.close()
        

    def agency_dataquality(self):
            
            parameters = ({"start_date": self.start_date, "end_date": self.end_date})
            
            name=dq.Name()
            ssn=dq.SSN()
            dob=dq.DOB()
            race=dq.Race()
            gender=dq.Gender()
            veteran=dq.Veteran()
            start=dq.StartDate()
            hoh=dq.HOH()
            coc=dq.Location()
            disability=dq.Disabling()
            destination=dq.Destination()
            income=dq.Income()
            
            agency_count="{:,}".format(dq.active_clients(**parameters))
            
            total_name_accuracy = name.name_total_accuracy(**parameters)
            client_name_refused = name.name_client_refused_doesnt_know(**parameters)
            missing_name = name.name_missing(**parameters)
            issues_name = name.name_data_accuracy(**parameters)
            
            total_name_percentage = "{:.1%}".format(total_name_accuracy)
            refused_name_percentage = "{:.1%}".format(1-client_name_refused)
            missing_name_percentage = "{:.1%}".format(1-missing_name)
            issues_name_percentage = "{:.1%}".format(1-issues_name)
            
            total_ssn_accuracy = ssn.ssn_total_accuracy(**parameters)
            client_refused_ssn = ssn.ssn_client_refused_doesnt_know(**parameters)
            missing_ssn = ssn.ssn_missing(**parameters)
            issues_ssn = ssn.ssn_data_accuracy(**parameters)
            
            total_ssn_percentage = "{:.1%}".format(total_ssn_accuracy)
            refused_ssn_percentage = "{:.1%}".format(1-client_refused_ssn)
            missing_ssn_percentage = "{:.1%}".format(1-missing_ssn)
            issues_ssn_percentage = "{:.1%}".format(1-issues_ssn)
            
            total_dob_accuracy = dob.dob_total_accuracy(**parameters)
            client_refused_dob = dob.dob_client_refused_doesnt_know(**parameters)
            missing_dob = dob.dob_missing(**parameters)
            issues_dob = dob.dob_data_accuracy(**parameters)
            
            total_dob_percentage = "{:.1%}".format(total_dob_accuracy)
            refused_dob_percentage = "{:.1%}".format(1-client_refused_dob)
            missing_dob_percentage = "{:.1%}".format(1-missing_dob)
            issues_dob_percentage = "{:.1%}".format(1-issues_dob)
            
            total_race_accuracy = race.race_total_accuracy(**parameters)
            client_refused_race = race.race_client_refused_doesnt_know(**parameters)
            missing_race = race.race_missing(**parameters)
            
            total_race_percentage = "{:.1%}".format(total_race_accuracy)
            refused_percentage_race = "{:.1%}".format(1-client_refused_race)
            missing_percentage_race = "{:.1%}".format(1-missing_race)
            
            total_gender_accuracy = gender.gender_total_accuracy(**parameters)
            client_refused_gender = gender.gender_client_refused_doesnt_know(**parameters)
            missing_gender = gender.gender_missing(**parameters)
            
            total_gender_percentage = "{:.1%}".format(total_gender_accuracy)
            refused_gender_percentage = "{:.1%}".format(1-client_refused_gender)
            missing_gender_percentage = "{:.1%}".format(1-missing_gender)
            
            total_veteran_accuracy = veteran.veteran_total_accuracy(**parameters)
            client_refused_veteran = veteran.veteran_client_refused_doesnt_know(**parameters)
            missing_veteran = veteran.veteran_missing(**parameters)
            issues_veteran = veteran.veteran_data_accuracy(**parameters)
            
            total_veteran_percentage = "{:.1%}".format(total_veteran_accuracy)
            refused_veteran_percentage = "{:.1%}".format(1-client_refused_veteran)
            missing_veteran_percentage = "{:.1%}".format(1-missing_veteran)
            issues_veteran_percentage = "{:.1%}".format(1-issues_veteran)
            
            total_start_accuracy = start.start_date_data_accuracy(**parameters)
            total_start_percentage = "{:.1%}".format(total_start_accuracy)
            
            total_hoh_accuracy = hoh.HoH_total_accuracy(**parameters)
            missing_hoh = hoh.HoH_missing(**parameters)
            issues_hoh = hoh.HoH_data_accuracy(**parameters)

            total_hoh_percentage = "{:.1%}".format(total_hoh_accuracy)
            missing_hoh_percentage = "{:.1%}".format(1-missing_hoh)
            issues_hoh_percentage = "{:.1%}".format(1-issues_hoh)
            
            total_coc_accuracy = coc.enrollment_coc_total_accuracy(**parameters)
            missing_coc = coc.enrollment_coc_missing(**parameters)
            issues_coc = coc.enrollment_coc_data_accuracy(**parameters)

            total_coc_percentage = "{:.1%}".format(total_coc_accuracy)
            missing_coc_percentage = "{:.1%}".format(1-missing_coc)
            issues_coc_percentage = "{:.1%}".format(1-issues_coc)
            
            total_disability_accuracy = disability.disabling_condition_total_accuracy(**parameters)
            client_refused_disability= disability.disabling_condition_client_refused(**parameters)
            missing_disability = disability.disabling_condition_missing(**parameters)
            issues_disability = disability.disabling_condition_data_accuracy(**parameters)

            total_disability_percentage = "{:.1%}".format(total_disability_accuracy)
            refused_disability_percentage = "{:.1%}".format(1-client_refused_disability)
            missing_disability_percentage = "{:.1%}".format(1-missing_disability)
            issues_disability_percentage = "{:.1%}".format(1-issues_disability)
            
            total_destination_accuracy = destination.destination_total_accuracy(**parameters)
            client_refused_destination = destination.destination_client_refused_doesnt_know(**parameters)
            missing_destination = destination.destination_missing(**parameters)

            total_destination_percentage = "{:.1%}".format(total_destination_accuracy)
            refused_destination_percentage = "{:.1%}".format(1-client_refused_destination)
            missing_destination_percentage = "{:.1%}".format(1-missing_destination)
            
            total_start_income_accuracy = income.starting_income_total_accuracy(**parameters)
            client_refused_start_income= income.starting_income_client_refused(**parameters)
            missing_start_income = income.starting_income_missing(**parameters)
            issues_start_income = income.starting_income_data_accuracy(**parameters)

            total_start_income_percentage = "{:.1%}".format(total_start_income_accuracy)
            refused_start_income_percentage = "{:.1%}".format(1-client_refused_start_income)
            missing_start_income_percentage = "{:.1%}".format(1-missing_start_income)
            issues_start_income_percentage = "{:.1%}".format(1-issues_start_income)
            
            total_annual_income_accuracy = income.annual_income_total_accuracy(**parameters)
            client_refused_annual_income= income.annual_income_client_refused(**parameters)
            missing_annual_income = income.annual_income_missing(**parameters)
            issues_annual_income = income.annual_income_data_accuracy(**parameters)

            total_annual_income_percentage = "{:.1%}".format(total_annual_income_accuracy)
            refused_annual_income_percentage = "{:.1%}".format(1-client_refused_annual_income)
            missing_annual_income_percentage = "{:.1%}".format(1-missing_annual_income)
            issues_annual_income_percentage = "{:.1%}".format(1-issues_annual_income)
            
            total_exit_income_accuracy = income.exiting_income_total_accuracy(**parameters)
            client_refused_exit_income= income.exiting_income_client_refused(**parameters)
            missing_exit_income = income.exiting_income_client_missing(**parameters)
            issues_exit_income = income.exiting_income_data_accuracy(**parameters)

            total_exit_income_percentage = "{:.1%}".format(total_exit_income_accuracy)
            refused_exit_income_percentage = "{:.1%}".format(1-client_refused_exit_income)
            missing_exit_income_percentage = "{:.1%}".format(1-missing_exit_income)
            issues_exit_income_percentage = "{:.1%}".format(1-issues_exit_income)   


            
            header_text = [f"Agency Clients: {agency_count}", f" ", "Agency Goal","Agency Accuracy", "Client Refused", "Missing", "Data Issues"]
            formatted_header_text = [Paragraph(cell, rf.tableSecondaryHeader) for cell in header_text]

            # Create two rows of headers
            personal_data = [
                formatted_header_text,  # First row
                [Paragraph("Personal Data Quality", rf.tableSecondaryHeaderLeftBlue), 
                Paragraph("Applies To", rf.tableSecondaryHeaderLeftBlue), 
                "","","", "", ""] ] # Second row

            personal_data.append([Paragraph("Name", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(total_name_percentage,rf.nintyfive_cellColor_gradient(total_name_percentage)),Paragraph(refused_name_percentage, rf.tableValuesStyle),Paragraph(missing_name_percentage, rf.tableValuesStyle),Paragraph(issues_name_percentage, rf.tableValuesStyle)])
            personal_data.append([Paragraph("Social Security Number", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(total_ssn_percentage, rf.nintyfive_cellColor_gradient(total_ssn_percentage)),Paragraph(refused_ssn_percentage, rf.tableValuesStyle),Paragraph(missing_ssn_percentage, rf.tableValuesStyle),Paragraph(issues_ssn_percentage, rf.tableValuesStyle)])
            personal_data.append([Paragraph("Date of Birth", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(total_dob_percentage, rf.nintyfive_cellColor_gradient(total_dob_percentage)),Paragraph(refused_dob_percentage, rf.tableValuesStyle),Paragraph(missing_dob_percentage, rf.tableValuesStyle),Paragraph(issues_dob_percentage, rf.tableValuesStyle)])
            personal_data.append([Paragraph("Race & Ethnicity", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(total_race_percentage, rf.nintyfive_cellColor_gradient(total_race_percentage)),Paragraph(refused_percentage_race, rf.tableValuesStyle),Paragraph(missing_percentage_race, rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
            personal_data.append([Paragraph("Gender", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(total_gender_percentage, rf.nintyfive_cellColor_gradient(total_gender_percentage)),Paragraph(refused_gender_percentage, rf.tableValuesStyle),Paragraph(missing_gender_percentage, rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])

            universal_data = [ # First row
                [Paragraph("Universal Data Quality", rf.tableSecondaryHeaderLeftPurple), 
                Paragraph("Applies To", rf.tableSecondaryHeaderLeftPurple), 
                "","","","", ""] ] # Second row
            
            universal_data.append([Paragraph("Veteran Status", rf.tableTextStyle),Paragraph("All Adult Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(total_veteran_percentage, rf.ninty_cellColor_gradient(total_veteran_percentage)),Paragraph(refused_veteran_percentage, rf.tableValuesStyle),Paragraph(missing_veteran_percentage, rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
            universal_data.append([Paragraph("Project Start Date", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(total_start_percentage, rf.nintyfive_cellColor_gradient(total_start_percentage)),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
            universal_data.append([Paragraph("Head of Household", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(total_hoh_percentage, rf.ninty_cellColor_gradient(total_hoh_percentage)),Paragraph(" - ", rf.tableValuesStyle),Paragraph(missing_hoh_percentage, rf.tableValuesStyle),Paragraph(issues_hoh_percentage, rf.tableValuesStyle)])
            universal_data.append([Paragraph("Continuum of Care", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(total_coc_percentage, rf.nintyfive_cellColor_gradient(total_coc_percentage)),Paragraph(" - ", rf.tableValuesStyle),Paragraph(missing_coc_percentage, rf.tableValuesStyle),Paragraph(issues_coc_percentage, rf.tableValuesStyle)])
            universal_data.append([Paragraph("Disability Status", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(total_disability_percentage, rf.ninty_cellColor_gradient(total_disability_percentage)),Paragraph(refused_disability_percentage, rf.tableValuesStyle),Paragraph(missing_disability_percentage, rf.tableValuesStyle),Paragraph(issues_disability_percentage, rf.tableValuesStyle)])

            income_data = [ # First row
                [Paragraph("Income & Housing Data Quality", rf.tableSecondaryHeaderLeftGreen), 
                Paragraph("Applies To", rf.tableSecondaryHeaderLeftGreen), 
                "","","", "", ""] ] # Second row
            
            income_data.append([Paragraph("Exit Destination", rf.tableTextStyle),Paragraph("All Exited Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(total_destination_percentage, rf.nintyfive_cellColor_gradient(total_destination_percentage)),Paragraph(refused_destination_percentage, rf.tableValuesStyle),Paragraph(missing_destination_percentage, rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
            income_data.append([Paragraph("Income at Entry", rf.tableTextStyle),Paragraph("All Adult Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(total_start_income_percentage, rf.ninty_cellColor_gradient(total_start_income_percentage)),Paragraph(refused_start_income_percentage, rf.tableValuesStyle),Paragraph(missing_start_income_percentage, rf.tableValuesStyle),Paragraph(issues_start_income_percentage, rf.tableValuesStyle)])
            income_data.append([Paragraph("Income at Annual Assessment", rf.tableTextStyle),Paragraph("All Adult Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(total_annual_income_percentage, rf.ninty_cellColor_gradient(total_annual_income_percentage)),Paragraph(refused_annual_income_percentage, rf.tableValuesStyle),Paragraph(missing_annual_income_percentage, rf.tableValuesStyle),Paragraph(issues_annual_income_percentage, rf.tableValuesStyle)])
            income_data.append([Paragraph("Income at Exit", rf.tableTextStyle),Paragraph("All Adult Exited Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(total_exit_income_percentage, rf.ninty_cellColor_gradient(total_exit_income_percentage)),Paragraph(refused_exit_income_percentage, rf.tableValuesStyle),Paragraph(missing_exit_income_percentage, rf.tableValuesStyle),Paragraph(issues_exit_income_percentage, rf.tableValuesStyle)])

            personal_table = Table(personal_data,colWidths=[1.75*inch,1.5*inch,1.1*inch,1*inch,.7*inch,.7*inch,.7*inch],style=rf.demosTableStyle)
            universal_table=Table(universal_data,colWidths=[1.75*inch,1.5*inch,1.1*inch,1*inch,.7*inch,.7*inch,.7*inch],style=rf.demosTableStylewhite)
            income_table=Table(income_data,colWidths=[1.75*inch,1.5*inch,1.1*inch,1*inch,.7*inch,.7*inch,.7*inch],style=rf.demosTableStylewhite)
            row1demotable = Table([[personal_table],[universal_table],[income_table]],style=rf.demoPageTableStyle)
            
            page1_demographics = [[row1demotable]
                                ]
            page1_demographics_table = Table(page1_demographics, style=rf.demoPageTableStyle)
            self.elements.append(page1_demographics_table)
            
    def executive_department_dataquality(self, department=None):
            
            parameters = ({"start_date": self.start_date, "end_date": self.end_date})
            
            name=dq.Name()
            ssn=dq.SSN()
            dob=dq.DOB()
            race=dq.Race()
            gender=dq.Gender()
            veteran=dq.Veteran()
            start=dq.StartDate()
            hoh=dq.HOH()
            coc=dq.Location()
            disability=dq.Disabling()
            destination=dq.Destination()
            income=dq.Income()
            
            agency_count="{:,}".format(dq.active_clients(**parameters))
            
            agency_name_accuracy = name.name_total_accuracy(**parameters)            
            agency_name_percentage = "{:.1%}".format(agency_name_accuracy)
            
            agency_ssn_accuracy = ssn.ssn_total_accuracy(**parameters)
            agency_ssn_percentage = "{:.1%}".format(agency_ssn_accuracy)
            
            agency_dob_accuracy = dob.dob_total_accuracy(**parameters)
            agency_dob_percentage = "{:.1%}".format(agency_dob_accuracy)

            agency_race_accuracy = race.race_total_accuracy(**parameters)
            agency_race_percentage = "{:.1%}".format(agency_race_accuracy)

            agency_gender_accuracy = gender.gender_total_accuracy(**parameters)
            agency_gender_percentage = "{:.1%}".format(agency_gender_accuracy)

            agency_veteran_accuracy = veteran.veteran_total_accuracy(**parameters)
            agency_veteran_percentage = "{:.1%}".format(agency_veteran_accuracy)
            
            agency_start_accuracy = start.start_date_data_accuracy(**parameters)
            agency_start_percentage = "{:.1%}".format(agency_start_accuracy)
            
            agency_hoh_accuracy = hoh.HoH_total_accuracy(**parameters)
            agency_hoh_percentage = "{:.1%}".format(agency_hoh_accuracy)
             
            agency_coc_accuracy = coc.enrollment_coc_total_accuracy(**parameters)
            agency_coc_percentage = "{:.1%}".format(agency_coc_accuracy)
            
            agency_disability_accuracy = disability.disabling_condition_total_accuracy(**parameters)
            agency_disability_percentage = "{:.1%}".format(agency_disability_accuracy)

            agency_destination_accuracy = destination.destination_total_accuracy(**parameters)
            agency_destination_percentage = "{:.1%}".format(agency_destination_accuracy)
            
            agency_start_income_accuracy=income.starting_income_total_accuracy(**parameters)
            agency_start_income_percentage="{:.1%}".format(agency_start_income_accuracy)
            
            agency_annual_income_accuracy=income.annual_income_total_accuracy(**parameters)
            agency_annual_income_percentage="{:.1%}".format(agency_annual_income_accuracy)
            
            agency_exit_income_accuracy=income.exiting_income_total_accuracy(**parameters)
            agency_exit_income_percentage="{:.1%}".format(agency_exit_income_accuracy)

            
  
            if department:
                title = department
                parameters.update({"department":[department]})

            #print(parameters)
            department_count="{:,}".format(dq.active_clients(**parameters))
            
            total_name_accuracy = name.name_total_accuracy(**parameters)
            client_name_refused = name.name_client_refused_doesnt_know(**parameters)
            missing_name = name.name_missing(**parameters)
            issues_name = name.name_data_accuracy(**parameters)
            
            total_name_percentage = "{:.1%}".format(total_name_accuracy)
            refused_name_percentage = "{:.1%}".format(1-client_name_refused)
            missing_name_percentage = "{:.1%}".format(1-missing_name)
            issues_name_percentage = "{:.1%}".format(1-issues_name)
            
            total_ssn_accuracy = ssn.ssn_total_accuracy(**parameters)
            client_refused_ssn = ssn.ssn_client_refused_doesnt_know(**parameters)
            missing_ssn = ssn.ssn_missing(**parameters)
            issues_ssn = ssn.ssn_data_accuracy(**parameters)
            
            total_ssn_percentage = "{:.1%}".format(total_ssn_accuracy)
            refused_ssn_percentage = "{:.1%}".format(1-client_refused_ssn)
            missing_ssn_percentage = "{:.1%}".format(1-missing_ssn)
            issues_ssn_percentage = "{:.1%}".format(1-issues_ssn)
            
            total_dob_accuracy = dob.dob_total_accuracy(**parameters)
            client_refused_dob = dob.dob_client_refused_doesnt_know(**parameters)
            missing_dob = dob.dob_missing(**parameters)
            issues_dob = dob.dob_data_accuracy(**parameters)
            
            total_dob_percentage = "{:.1%}".format(total_dob_accuracy)
            refused_dob_percentage = "{:.1%}".format(1-client_refused_dob)
            missing_dob_percentage = "{:.1%}".format(1-missing_dob)
            issues_dob_percentage = "{:.1%}".format(1-issues_dob)
            
            total_race_accuracy = race.race_total_accuracy(**parameters)
            client_refused_race = race.race_client_refused_doesnt_know(**parameters)
            missing_race = race.race_missing(**parameters)
            
            total_race_percentage = "{:.1%}".format(total_race_accuracy)
            refused_percentage_race = "{:.1%}".format(1-client_refused_race)
            missing_percentage_race = "{:.1%}".format(1-missing_race)
            
            total_gender_accuracy = gender.gender_total_accuracy(**parameters)
            client_refused_gender = gender.gender_client_refused_doesnt_know(**parameters)
            missing_gender = gender.gender_missing(**parameters)
            
            total_gender_percentage = "{:.1%}".format(total_gender_accuracy)
            refused_gender_percentage = "{:.1%}".format(1-client_refused_gender)
            missing_gender_percentage = "{:.1%}".format(1-missing_gender)
            
            total_veteran_accuracy = veteran.veteran_total_accuracy(**parameters)
            client_refused_veteran = veteran.veteran_client_refused_doesnt_know(**parameters)
            missing_veteran = veteran.veteran_missing(**parameters)
            issues_veteran = veteran.veteran_data_accuracy(**parameters)
            
            total_veteran_percentage = "{:.1%}".format(total_veteran_accuracy)
            refused_veteran_percentage = "{:.1%}".format(1-client_refused_veteran)
            missing_veteran_percentage = "{:.1%}".format(1-missing_veteran)
            issues_veteran_percentage = "{:.1%}".format(1-issues_veteran)
            
            total_start_accuracy = start.start_date_data_accuracy(**parameters)
            total_start_percentage = "{:.1%}".format(total_start_accuracy)
            
            total_hoh_accuracy = hoh.HoH_total_accuracy(**parameters)
            missing_hoh = hoh.HoH_missing(**parameters)
            issues_hoh = hoh.HoH_data_accuracy(**parameters)

            total_hoh_percentage = "{:.1%}".format(total_hoh_accuracy)
            missing_hoh_percentage = "{:.1%}".format(1-missing_hoh)
            issues_hoh_percentage = "{:.1%}".format(1-issues_hoh)
            
            total_coc_accuracy = coc.enrollment_coc_total_accuracy(**parameters)
            missing_coc = coc.enrollment_coc_missing(**parameters)
            issues_coc = coc.enrollment_coc_data_accuracy(**parameters)

            total_coc_percentage = "{:.1%}".format(total_coc_accuracy)
            missing_coc_percentage = "{:.1%}".format(1-missing_coc)
            issues_coc_percentage = "{:.1%}".format(1-issues_coc)
            
            total_disability_accuracy = disability.disabling_condition_total_accuracy(**parameters)
            client_refused_disability= disability.disabling_condition_client_refused(**parameters)
            missing_disability = disability.disabling_condition_missing(**parameters)
            issues_disability = disability.disabling_condition_data_accuracy(**parameters)

            total_disability_percentage = "{:.1%}".format(total_disability_accuracy)
            refused_disability_percentage = "{:.1%}".format(1-client_refused_disability)
            missing_disability_percentage = "{:.1%}".format(1-missing_disability)
            issues_disability_percentage = "{:.1%}".format(1-issues_disability)
            
            total_destination_accuracy = destination.destination_total_accuracy(**parameters)
            client_refused_destination = destination.destination_client_refused_doesnt_know(**parameters)
            missing_destination = destination.destination_missing(**parameters)

            total_destination_percentage = "{:.1%}".format(total_destination_accuracy)
            refused_destination_percentage = "{:.1%}".format(1-client_refused_destination)
            missing_destination_percentage = "{:.1%}".format(1-missing_destination)
            
            total_start_income_accuracy = income.starting_income_total_accuracy(**parameters)
            client_refused_start_income= income.starting_income_client_refused(**parameters)
            missing_start_income = income.starting_income_missing(**parameters)
            issues_start_income = income.starting_income_data_accuracy(**parameters)

            total_start_income_percentage = "{:.1%}".format(total_start_income_accuracy)
            refused_start_income_percentage = "{:.1%}".format(1-client_refused_start_income)
            missing_start_income_percentage = "{:.1%}".format(1-missing_start_income)
            issues_start_income_percentage = "{:.1%}".format(1-issues_start_income)
            
            total_annual_income_accuracy = income.annual_income_total_accuracy(**parameters)
            client_refused_annual_income= income.annual_income_client_refused(**parameters)
            missing_annual_income = income.annual_income_missing(**parameters)
            issues_annual_income = income.annual_income_data_accuracy(**parameters)

            total_annual_income_percentage = "{:.1%}".format(total_annual_income_accuracy)
            refused_annual_income_percentage = "{:.1%}".format(1-client_refused_annual_income)
            missing_annual_income_percentage = "{:.1%}".format(1-missing_annual_income)
            issues_annual_income_percentage = "{:.1%}".format(1-issues_annual_income)
            
            total_exit_income_accuracy = income.exiting_income_total_accuracy(**parameters)
            client_refused_exit_income= income.exiting_income_client_refused(**parameters)
            missing_exit_income = income.exiting_income_client_missing(**parameters)
            issues_exit_income = income.exiting_income_data_accuracy(**parameters)

            total_exit_income_percentage = "{:.1%}".format(total_exit_income_accuracy)
            refused_exit_income_percentage = "{:.1%}".format(1-client_refused_exit_income)
            missing_exit_income_percentage = "{:.1%}".format(1-missing_exit_income)
            issues_exit_income_percentage = "{:.1%}".format(1-issues_exit_income)
            if department== 'Permanent Supportive Services':
                department='PSS'
            
            header_text = [f"{department} Clients: {department_count}", f" ", "Agency Goal","Agency Accuracy","Dept. Accuracy", "Client Refused", "Missing", "Data Issues"]
            formatted_header_text = [Paragraph(cell, rf.tableSecondaryHeader) for cell in header_text]

            # Create two rows of headers
            personal_data = [
                formatted_header_text,  # First row
                [Paragraph("Personal Data Quality", rf.tableSecondaryHeaderLeftBlue), 
                Paragraph("Applies To", rf.tableSecondaryHeaderLeftBlue), 
                "","","", "", "", ""] ] # Second row

            personal_data.append([Paragraph("Name", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(agency_name_percentage, rf.tableValuesStylegray),Paragraph(total_name_percentage,rf.nintyfive_cellColor(total_name_percentage)),Paragraph(refused_name_percentage, rf.tableValuesStyle),Paragraph(missing_name_percentage, rf.tableValuesStyle),Paragraph(issues_name_percentage, rf.tableValuesStyle)])
            personal_data.append([Paragraph("Social Security Number", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(agency_ssn_percentage, rf.tableValuesStylegray),Paragraph(total_ssn_percentage, rf.nintyfive_cellColor(total_ssn_percentage)),Paragraph(refused_ssn_percentage, rf.tableValuesStyle),Paragraph(missing_ssn_percentage, rf.tableValuesStyle),Paragraph(issues_ssn_percentage, rf.tableValuesStyle)])
            personal_data.append([Paragraph("Date of Birth", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(agency_dob_percentage, rf.tableValuesStylegray),Paragraph(total_dob_percentage, rf.nintyfive_cellColor(total_dob_percentage)),Paragraph(refused_dob_percentage, rf.tableValuesStyle),Paragraph(missing_dob_percentage, rf.tableValuesStyle),Paragraph(issues_dob_percentage, rf.tableValuesStyle)])
            personal_data.append([Paragraph("Race & Ethnicity", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(agency_race_percentage, rf.tableValuesStylegray),Paragraph(total_race_percentage, rf.nintyfive_cellColor(total_race_percentage)),Paragraph(refused_percentage_race, rf.tableValuesStyle),Paragraph(missing_percentage_race, rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
            personal_data.append([Paragraph("Gender", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(agency_gender_percentage, rf.tableValuesStylegray),Paragraph(total_gender_percentage, rf.nintyfive_cellColor(total_gender_percentage)),Paragraph(refused_gender_percentage, rf.tableValuesStyle),Paragraph(missing_gender_percentage, rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])

            universal_data = [ # First row
                [Paragraph("Universal Data Quality", rf.tableSecondaryHeaderLeftPurple), 
                Paragraph("Applies To", rf.tableSecondaryHeaderLeftPurple), 
                "","","", "", "", ""] ] # Second row
            
            universal_data.append([Paragraph("Veteran Status", rf.tableTextStyle),Paragraph("All Adult Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(agency_veteran_percentage, rf.tableValuesStylegray),Paragraph(total_veteran_percentage, rf.ninty_cellColor(total_veteran_percentage)),Paragraph(refused_veteran_percentage, rf.tableValuesStyle),Paragraph(missing_veteran_percentage, rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
            universal_data.append([Paragraph("Project Start Date", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(agency_start_percentage, rf.tableValuesStylegray),Paragraph(total_start_percentage, rf.nintyfive_cellColor(total_start_percentage)),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
            universal_data.append([Paragraph("Head of Household", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(agency_hoh_percentage, rf.tableValuesStylegray),Paragraph(total_hoh_percentage, rf.ninty_cellColor(total_hoh_percentage)),Paragraph(" - ", rf.tableValuesStyle),Paragraph(missing_hoh_percentage, rf.tableValuesStyle),Paragraph(issues_hoh_percentage, rf.tableValuesStyle)])
            universal_data.append([Paragraph("Continuum of Care", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(agency_coc_percentage, rf.tableValuesStylegray),Paragraph(total_coc_percentage, rf.nintyfive_cellColor(total_coc_percentage)),Paragraph(" - ", rf.tableValuesStyle),Paragraph(missing_coc_percentage, rf.tableValuesStyle),Paragraph(issues_coc_percentage, rf.tableValuesStyle)])
            universal_data.append([Paragraph("Disability Status", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(agency_disability_percentage, rf.tableValuesStylegray),Paragraph(total_disability_percentage, rf.ninty_cellColor(total_disability_percentage)),Paragraph(refused_disability_percentage, rf.tableValuesStyle),Paragraph(missing_disability_percentage, rf.tableValuesStyle),Paragraph(issues_disability_percentage, rf.tableValuesStyle)])

            income_data = [ # First row
                [Paragraph("Income & Housing Data Quality", rf.tableSecondaryHeaderLeftGreen), 
                Paragraph("Applies To", rf.tableSecondaryHeaderLeftGreen), 
                "","", "", "","", ""] ] # Second row
            
            income_data.append([Paragraph("Exit Destination", rf.tableTextStyle),Paragraph("All Exited Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(agency_destination_percentage, rf.tableValuesStylegray),Paragraph(total_destination_percentage, rf.nintyfive_cellColor(total_destination_percentage)),Paragraph(refused_destination_percentage, rf.tableValuesStyle),Paragraph(missing_destination_percentage, rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
            income_data.append([Paragraph("Income at Entry", rf.tableTextStyle),Paragraph("All Adult Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(agency_start_income_percentage, rf.tableValuesStylegray),Paragraph(total_start_income_percentage, rf.ninty_cellColor(total_start_income_percentage)),Paragraph(refused_start_income_percentage, rf.tableValuesStyle),Paragraph(missing_start_income_percentage, rf.tableValuesStyle),Paragraph(issues_start_income_percentage, rf.tableValuesStyle)])
            income_data.append([Paragraph("Income at Annual Assessment", rf.tableTextStyle),Paragraph("All Adult Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(agency_annual_income_percentage, rf.tableValuesStylegray),Paragraph(total_annual_income_percentage, rf.ninty_cellColor(total_annual_income_percentage)),Paragraph(refused_annual_income_percentage, rf.tableValuesStyle),Paragraph(missing_annual_income_percentage, rf.tableValuesStyle),Paragraph(issues_annual_income_percentage, rf.tableValuesStyle)])
            income_data.append([Paragraph("Income at Exit", rf.tableTextStyle),Paragraph("All Adult Exited Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(agency_exit_income_percentage, rf.tableValuesStylegray),Paragraph(total_exit_income_percentage, rf.ninty_cellColor(total_exit_income_percentage)),Paragraph(refused_exit_income_percentage, rf.tableValuesStyle),Paragraph(missing_exit_income_percentage, rf.tableValuesStyle),Paragraph(issues_exit_income_percentage, rf.tableValuesStyle)])

            personal_table = Table(personal_data,colWidths=[1.75*inch,1.15*inch,.8*inch,.8*inch,.8*inch,.7*inch,.7*inch,.7*inch],style=rf.demosTableStyle)
            universal_table=Table(universal_data,colWidths=[1.75*inch,1.15*inch,.8*inch,.8*inch,.8*inch,.7*inch,.7*inch,.7*inch],style=rf.demosTableStylewhite)
            income_table=Table(income_data,colWidths=[1.75*inch,1.15*inch,.8*inch,.8*inch,.8*inch,.7*inch,.7*inch,.7*inch],style=rf.demosTableStylewhite)
            row1demotable = Table([[personal_table],[universal_table],[income_table]],style=rf.demoPageTableStyle)
            
            page1_demographics = [[row1demotable]
                                ]
            page1_demographics_table = Table(page1_demographics, style=rf.demoPageTableStyle)
            self.elements.append(page1_demographics_table)
    
    def department_dataquality(self, region=None, department=None,program_id=None):
            
            parameters = ({"start_date": self.start_date, "end_date": self.end_date})
            
            name=dq.Name()
            ssn=dq.SSN()
            dob=dq.DOB()
            race=dq.Race()
            gender=dq.Gender()
            veteran=dq.Veteran()
            start=dq.StartDate()
            hoh=dq.HOH()
            coc=dq.Location()
            disability=dq.Disabling()
            destination=dq.Destination()
            income=dq.Income()
            
            agency_count="{:,}".format(dq.active_clients(**parameters))
            
            agency_name_accuracy = name.name_total_accuracy(**parameters)            
            agency_name_percentage = "{:.1%}".format(agency_name_accuracy)
            
            agency_ssn_accuracy = ssn.ssn_total_accuracy(**parameters)
            agency_ssn_percentage = "{:.1%}".format(agency_ssn_accuracy)
            
            agency_dob_accuracy = dob.dob_total_accuracy(**parameters)
            agency_dob_percentage = "{:.1%}".format(agency_dob_accuracy)

            agency_race_accuracy = race.race_total_accuracy(**parameters)
            agency_race_percentage = "{:.1%}".format(agency_race_accuracy)

            agency_gender_accuracy = gender.gender_total_accuracy(**parameters)
            agency_gender_percentage = "{:.1%}".format(agency_gender_accuracy)

            agency_veteran_accuracy = veteran.veteran_total_accuracy(**parameters)
            agency_veteran_percentage = "{:.1%}".format(agency_veteran_accuracy)
            
            agency_start_accuracy = start.start_date_data_accuracy(**parameters)
            agency_start_percentage = "{:.1%}".format(agency_start_accuracy)
            
            agency_hoh_accuracy = hoh.HoH_total_accuracy(**parameters)
            agency_hoh_percentage = "{:.1%}".format(agency_hoh_accuracy)
             
            agency_coc_accuracy = coc.enrollment_coc_total_accuracy(**parameters)
            agency_coc_percentage = "{:.1%}".format(agency_coc_accuracy)
            
            agency_disability_accuracy = disability.disabling_condition_total_accuracy(**parameters)
            agency_disability_percentage = "{:.1%}".format(agency_disability_accuracy)

            agency_destination_accuracy = destination.destination_total_accuracy(**parameters)
            agency_destination_percentage = "{:.1%}".format(agency_destination_accuracy)
            
            agency_start_income_accuracy=income.starting_income_total_accuracy(**parameters)
            agency_start_income_percentage="{:.1%}".format(agency_start_income_accuracy)
            
            agency_annual_income_accuracy=income.annual_income_total_accuracy(**parameters)
            agency_annual_income_percentage="{:.1%}".format(agency_annual_income_accuracy)
            
            agency_exit_income_accuracy=income.exiting_income_total_accuracy(**parameters)
            agency_exit_income_percentage="{:.1%}".format(agency_exit_income_accuracy)

            
            if region:
                title = region
                parameters.update({"region":[region]})
            elif department:
                title = department
                parameters.update({"department":[department]})
            elif program_id:
                parameters.update({"program_id":[program_id]})

            print(parameters)
            department_count="{:,}".format(dq.active_clients(**parameters))
            
            total_name_accuracy = name.name_total_accuracy(**parameters)
            client_name_refused = name.name_client_refused_doesnt_know(**parameters)
            missing_name = name.name_missing(**parameters)
            issues_name = name.name_data_accuracy(**parameters)
            
            total_name_percentage = "{:.1%}".format(total_name_accuracy)
            refused_name_percentage = "{:.1%}".format(1-client_name_refused)
            missing_name_percentage = "{:.1%}".format(1-missing_name)
            issues_name_percentage = "{:.1%}".format(1-issues_name)
            
            total_ssn_accuracy = ssn.ssn_total_accuracy(**parameters)
            client_refused_ssn = ssn.ssn_client_refused_doesnt_know(**parameters)
            missing_ssn = ssn.ssn_missing(**parameters)
            issues_ssn = ssn.ssn_data_accuracy(**parameters)
            
            total_ssn_percentage = "{:.1%}".format(total_ssn_accuracy)
            refused_ssn_percentage = "{:.1%}".format(1-client_refused_ssn)
            missing_ssn_percentage = "{:.1%}".format(1-missing_ssn)
            issues_ssn_percentage = "{:.1%}".format(1-issues_ssn)
            
            total_dob_accuracy = dob.dob_total_accuracy(**parameters)
            client_refused_dob = dob.dob_client_refused_doesnt_know(**parameters)
            missing_dob = dob.dob_missing(**parameters)
            issues_dob = dob.dob_data_accuracy(**parameters)
            
            total_dob_percentage = "{:.1%}".format(total_dob_accuracy)
            refused_dob_percentage = "{:.1%}".format(1-client_refused_dob)
            missing_dob_percentage = "{:.1%}".format(1-missing_dob)
            issues_dob_percentage = "{:.1%}".format(1-issues_dob)
            
            total_race_accuracy = race.race_total_accuracy(**parameters)
            client_refused_race = race.race_client_refused_doesnt_know(**parameters)
            missing_race = race.race_missing(**parameters)
            
            total_race_percentage = "{:.1%}".format(total_race_accuracy)
            refused_percentage_race = "{:.1%}".format(1-client_refused_race)
            missing_percentage_race = "{:.1%}".format(1-missing_race)
            
            total_gender_accuracy = gender.gender_total_accuracy(**parameters)
            client_refused_gender = gender.gender_client_refused_doesnt_know(**parameters)
            missing_gender = gender.gender_missing(**parameters)
            
            total_gender_percentage = "{:.1%}".format(total_gender_accuracy)
            refused_gender_percentage = "{:.1%}".format(1-client_refused_gender)
            missing_gender_percentage = "{:.1%}".format(1-missing_gender)
            
            total_veteran_accuracy = veteran.veteran_total_accuracy(**parameters)
            client_refused_veteran = veteran.veteran_client_refused_doesnt_know(**parameters)
            missing_veteran = veteran.veteran_missing(**parameters)
            issues_veteran = veteran.veteran_data_accuracy(**parameters)
            
            total_veteran_percentage = "{:.1%}".format(total_veteran_accuracy)
            refused_veteran_percentage = "{:.1%}".format(1-client_refused_veteran)
            missing_veteran_percentage = "{:.1%}".format(1-missing_veteran)
            issues_veteran_percentage = "{:.1%}".format(1-issues_veteran)
            
            total_start_accuracy = start.start_date_data_accuracy(**parameters)
            total_start_percentage = "{:.1%}".format(total_start_accuracy)
            
            total_hoh_accuracy = hoh.HoH_total_accuracy(**parameters)
            missing_hoh = hoh.HoH_missing(**parameters)
            issues_hoh = hoh.HoH_data_accuracy(**parameters)

            total_hoh_percentage = "{:.1%}".format(total_hoh_accuracy)
            missing_hoh_percentage = "{:.1%}".format(1-missing_hoh)
            issues_hoh_percentage = "{:.1%}".format(1-issues_hoh)
            
            total_coc_accuracy = coc.enrollment_coc_total_accuracy(**parameters)
            missing_coc = coc.enrollment_coc_missing(**parameters)
            issues_coc = coc.enrollment_coc_data_accuracy(**parameters)

            total_coc_percentage = "{:.1%}".format(total_coc_accuracy)
            missing_coc_percentage = "{:.1%}".format(1-missing_coc)
            issues_coc_percentage = "{:.1%}".format(1-issues_coc)
            
            total_disability_accuracy = disability.disabling_condition_total_accuracy(**parameters)
            client_refused_disability= disability.disabling_condition_client_refused(**parameters)
            missing_disability = disability.disabling_condition_missing(**parameters)
            issues_disability = disability.disabling_condition_data_accuracy(**parameters)

            total_disability_percentage = "{:.1%}".format(total_disability_accuracy)
            refused_disability_percentage = "{:.1%}".format(1-client_refused_disability)
            missing_disability_percentage = "{:.1%}".format(1-missing_disability)
            issues_disability_percentage = "{:.1%}".format(1-issues_disability)
            
            total_destination_accuracy = destination.destination_total_accuracy(**parameters)
            client_refused_destination = destination.destination_client_refused_doesnt_know(**parameters)
            missing_destination = destination.destination_missing(**parameters)

            total_destination_percentage = "{:.1%}".format(total_destination_accuracy)
            refused_destination_percentage = "{:.1%}".format(1-client_refused_destination)
            missing_destination_percentage = "{:.1%}".format(1-missing_destination)
            
            total_start_income_accuracy = income.starting_income_total_accuracy(**parameters)
            client_refused_start_income= income.starting_income_client_refused(**parameters)
            missing_start_income = income.starting_income_missing(**parameters)
            issues_start_income = income.starting_income_data_accuracy(**parameters)

            total_start_income_percentage = "{:.1%}".format(total_start_income_accuracy)
            refused_start_income_percentage = "{:.1%}".format(1-client_refused_start_income)
            missing_start_income_percentage = "{:.1%}".format(1-missing_start_income)
            issues_start_income_percentage = "{:.1%}".format(1-issues_start_income)
            
            total_annual_income_accuracy = income.annual_income_total_accuracy(**parameters)
            client_refused_annual_income= income.annual_income_client_refused(**parameters)
            missing_annual_income = income.annual_income_missing(**parameters)
            issues_annual_income = income.annual_income_data_accuracy(**parameters)

            total_annual_income_percentage = "{:.1%}".format(total_annual_income_accuracy)
            refused_annual_income_percentage = "{:.1%}".format(1-client_refused_annual_income)
            missing_annual_income_percentage = "{:.1%}".format(1-missing_annual_income)
            issues_annual_income_percentage = "{:.1%}".format(1-issues_annual_income)
            
            total_exit_income_accuracy = income.exiting_income_total_accuracy(**parameters)
            client_refused_exit_income= income.exiting_income_client_refused(**parameters)
            missing_exit_income = income.exiting_income_client_missing(**parameters)
            issues_exit_income = income.exiting_income_data_accuracy(**parameters)

            total_exit_income_percentage = "{:.1%}".format(total_exit_income_accuracy)
            refused_exit_income_percentage = "{:.1%}".format(1-client_refused_exit_income)
            missing_exit_income_percentage = "{:.1%}".format(1-missing_exit_income)
            issues_exit_income_percentage = "{:.1%}".format(1-issues_exit_income)
            if department== 'Permanent Supportive Services':
                department='PSS'
            
            header_text = [f"{department} Clients: {department_count}", f"Agency Clients: {agency_count}","Agency Goal", "Agency Accuracy","Dept. Accuracy", "Client Refused", "Missing", "Data Issues"]
            formatted_header_text = [Paragraph(cell, rf.tableSecondaryHeader) for cell in header_text]

            # Create two rows of headers
            personal_data = [
                formatted_header_text,  # First row
                [Paragraph("Personal Data Quality", rf.tableSecondaryHeaderLeftBlue), 
                Paragraph("Applies To", rf.tableSecondaryHeaderLeftBlue), 
                "","", ""," ", "", ""] ] # Second row

            personal_data.append([Paragraph("Name", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(agency_name_percentage, rf.tableValuesStylegray),Paragraph(total_name_percentage,rf.nintyfive_cellColor_gradient(total_name_percentage)),Paragraph(refused_name_percentage, rf.tableValuesStyle),Paragraph(missing_name_percentage, rf.tableValuesStyle),Paragraph(issues_name_percentage, rf.tableValuesStyle)])
            personal_data.append([Paragraph("Social Security Number", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(agency_ssn_percentage, rf.tableValuesStylegray),Paragraph(total_ssn_percentage, rf.nintyfive_cellColor_gradient(total_ssn_percentage)),Paragraph(refused_ssn_percentage, rf.tableValuesStyle),Paragraph(missing_ssn_percentage, rf.tableValuesStyle),Paragraph(issues_ssn_percentage, rf.tableValuesStyle)])
            personal_data.append([Paragraph("Date of Birth", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(agency_dob_percentage, rf.tableValuesStylegray),Paragraph(total_dob_percentage, rf.nintyfive_cellColor_gradient(total_dob_percentage)),Paragraph(refused_dob_percentage, rf.tableValuesStyle),Paragraph(missing_dob_percentage, rf.tableValuesStyle),Paragraph(issues_dob_percentage, rf.tableValuesStyle)])
            personal_data.append([Paragraph("Race & Ethnicity", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(agency_race_percentage, rf.tableValuesStylegray),Paragraph(total_race_percentage, rf.nintyfive_cellColor_gradient(total_race_percentage)),Paragraph(refused_percentage_race, rf.tableValuesStyle),Paragraph(missing_percentage_race, rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
            personal_data.append([Paragraph("Gender", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(agency_gender_percentage, rf.tableValuesStylegray),Paragraph(total_gender_percentage, rf.nintyfive_cellColor_gradient(total_gender_percentage)),Paragraph(refused_gender_percentage, rf.tableValuesStyle),Paragraph(missing_gender_percentage, rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])

            universal_data = [ # First row
                [Paragraph("Universal Data Quality", rf.tableSecondaryHeaderLeftPurple), 
                Paragraph("Applies To", rf.tableSecondaryHeaderLeftPurple), 
                "","", "", "","", ""] ] # Second row
            
            universal_data.append([Paragraph("Veteran Status", rf.tableTextStyle),Paragraph("All Adult Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(agency_veteran_percentage, rf.tableValuesStylegray),Paragraph(total_veteran_percentage, rf.ninty_cellColor_gradient(total_veteran_percentage)),Paragraph(refused_veteran_percentage, rf.tableValuesStyle),Paragraph(missing_veteran_percentage, rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
            universal_data.append([Paragraph("Project Start Date", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(agency_start_percentage, rf.tableValuesStylegray),Paragraph(total_start_percentage, rf.nintyfive_cellColor_gradient(total_start_percentage)),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
            universal_data.append([Paragraph("Head of Household", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(agency_hoh_percentage, rf.tableValuesStylegray),Paragraph(total_hoh_percentage, rf.ninty_cellColor_gradient(total_hoh_percentage)),Paragraph(" - ", rf.tableValuesStyle),Paragraph(missing_hoh_percentage, rf.tableValuesStyle),Paragraph(issues_hoh_percentage, rf.tableValuesStyle)])
            universal_data.append([Paragraph("Continuum of Care", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(agency_coc_percentage, rf.tableValuesStylegray),Paragraph(total_coc_percentage, rf.nintyfive_cellColor_gradient(total_coc_percentage)),Paragraph(" - ", rf.tableValuesStyle),Paragraph(missing_coc_percentage, rf.tableValuesStyle),Paragraph(issues_coc_percentage, rf.tableValuesStyle)])
            universal_data.append([Paragraph("Disability Status", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(agency_disability_percentage, rf.tableValuesStylegray),Paragraph(total_disability_percentage, rf.ninty_cellColor_gradient(total_disability_percentage)),Paragraph(refused_disability_percentage, rf.tableValuesStyle),Paragraph(missing_disability_percentage, rf.tableValuesStyle),Paragraph(issues_disability_percentage, rf.tableValuesStyle)])

            income_data = [ # First row
                [Paragraph("Income & Housing Data Quality", rf.tableSecondaryHeaderLeftGreen), 
                Paragraph("Applies To", rf.tableSecondaryHeaderLeftGreen), 
                "","", "","", "", ""] ] # Second row
            
            income_data.append([Paragraph("Exit Destination", rf.tableTextStyle),Paragraph("All Exited Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(agency_destination_percentage, rf.tableValuesStylegray),Paragraph(total_destination_percentage, rf.nintyfive_cellColor_gradient(total_destination_percentage)),Paragraph(refused_destination_percentage, rf.tableValuesStyle),Paragraph(missing_destination_percentage, rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
            income_data.append([Paragraph("Income at Entry", rf.tableTextStyle),Paragraph("All Adult Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(agency_start_income_percentage, rf.tableValuesStylegray),Paragraph(total_start_income_percentage, rf.ninty_cellColor_gradient(total_start_income_percentage)),Paragraph(refused_start_income_percentage, rf.tableValuesStyle),Paragraph(missing_start_income_percentage, rf.tableValuesStyle),Paragraph(issues_start_income_percentage, rf.tableValuesStyle)])
            income_data.append([Paragraph("Income at Annual Assessment", rf.tableTextStyle),Paragraph("All Adult Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(agency_annual_income_percentage, rf.tableValuesStylegray),Paragraph(total_annual_income_percentage, rf.ninty_cellColor_gradient(total_annual_income_percentage)),Paragraph(refused_annual_income_percentage, rf.tableValuesStyle),Paragraph(missing_annual_income_percentage, rf.tableValuesStyle),Paragraph(issues_annual_income_percentage, rf.tableValuesStyle)])
            income_data.append([Paragraph("Income at Exit", rf.tableTextStyle),Paragraph("All Adult Exited Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(agency_exit_income_percentage, rf.tableValuesStylegray),Paragraph(total_exit_income_percentage, rf.ninty_cellColor_gradient(total_exit_income_percentage)),Paragraph(refused_exit_income_percentage, rf.tableValuesStyle),Paragraph(missing_exit_income_percentage, rf.tableValuesStyle),Paragraph(issues_exit_income_percentage, rf.tableValuesStyle)])

            personal_table = Table(personal_data,colWidths=[1.75*inch,1.15*inch,.8*inch,.8*inch,.8*inch,.7*inch,.7*inch,.7*inch],style=rf.demosTableStyle)
            universal_table=Table(universal_data,colWidths=[1.75*inch,1.15*inch,.8*inch,.8*inch,.8*inch,.7*inch,.7*inch,.7*inch],style=rf.demosTableStylewhite)
            income_table=Table(income_data,colWidths=[1.75*inch,1.15*inch,.8*inch,.8*inch,.8*inch,.7*inch,.7*inch,.7*inch],style=rf.demosTableStylewhite)
            row1demotable = Table([[personal_table],[universal_table],[income_table]],style=rf.demoPageTableStyle)
            
            page1_demographics = [[row1demotable]
                                ]
            page1_demographics_table = Table(page1_demographics, style=rf.demoPageTableStyle)
            self.elements.append(page1_demographics_table)
    
    
    def department_timeliness(self, region=None, department=None,program_id=None):
            parameters = ({"start_date": self.start_date, "end_date": self.end_date})
            time=dq.Timeliness()            
            agency_1_day = time.percent_start_records_created_within_x_days(**parameters, days=1)
            agency_4_day = time.percent_start_records_created_within_x_days(**parameters, days=4)
            agency_7_day = time.percent_start_records_created_within_x_days(**parameters, days=7)
            agency_11_day = time.percent_start_records_created_within_x_days(**parameters, days=11)
            agency_14_day = time.percent_start_records_created_within_x_days(**parameters, days=14)
            
            agency_1_day_percentage = "{:.1%}".format(agency_1_day)
            agency_4_day_percentage = "{:.1%}".format(agency_4_day)
            agency_7_day_percentage = "{:.1%}".format(agency_7_day)
            agency_11_day_percentage = "{:.1%}".format(agency_11_day)
            agency_14_day_percentage = "{:.1%}".format(agency_14_day)
            
            agency_1_day_exit = time.percent_exit_records_created_within_x_days(**parameters, days=1)
            agency_4_day_exit = time.percent_exit_records_created_within_x_days(**parameters, days=4)
            agency_7_day_exit = time.percent_exit_records_created_within_x_days(**parameters, days=7)
            agency_11_day_exit = time.percent_exit_records_created_within_x_days(**parameters, days=11)
            agency_14_day_exit = time.percent_exit_records_created_within_x_days(**parameters, days=14)
            
            agency_1_day_percentage_exit = "{:.1%}".format(agency_1_day_exit)
            agency_4_day_percentage_exit = "{:.1%}".format(agency_4_day_exit)
            agency_7_day_percentage_exit = "{:.1%}".format(agency_7_day_exit)
            agency_11_day_percentage_exit = "{:.1%}".format(agency_11_day_exit)
            agency_14_day_percentage_exit = "{:.1%}".format(agency_14_day_exit)
            
            if region:
                title = region
                parameters.update({"region":[region]})
            elif department:
                title = department
                parameters.update({"department":[department]})
            elif program_id:
                parameters.update({"program_id":[program_id]})


            def get_image(path, width=4*inch):
                img = utils.ImageReader(path)
                iw, ih = img.getSize()
                aspect = ih / float(iw)
                return Image(path, width=width, height=(width * aspect)*.9)

            
            ##time-start
            total_1_day = time.percent_start_records_created_within_x_days(**parameters, days=1)
            total_4_day = time.percent_start_records_created_within_x_days(**parameters, days=4)
            total_7_day = time.percent_start_records_created_within_x_days(**parameters, days=7)
            total_11_day = time.percent_start_records_created_within_x_days(**parameters, days=11)
            total_14_day = time.percent_start_records_created_within_x_days(**parameters, days=14)
            
            day_1_percentage = "{:.1%}".format(total_1_day)
            day_4_percentage = "{:.1%}".format(total_4_day)
            day_7_percentage = "{:.1%}".format(total_7_day)
            day_11_percentage = "{:.1%}".format(total_11_day)
            day_14_percentage = "{:.1%}".format(total_14_day)
            
            ##time-exit
            total_1_day_exit = time.percent_exit_records_created_within_x_days(**parameters, days=1)
            total_4_day_exit = time.percent_exit_records_created_within_x_days(**parameters, days=4)
            total_7_day_exit = time.percent_exit_records_created_within_x_days(**parameters, days=7)
            total_11_day_exit = time.percent_exit_records_created_within_x_days(**parameters, days=11)
            total_14_day_exit = time.percent_exit_records_created_within_x_days(**parameters, days=14)
        
            day_1_percentage_exit = "{:.1%}".format(total_1_day_exit)
            day_4_percentage_exit = "{:.1%}".format(total_4_day_exit)
            day_7_percentage_exit = "{:.1%}".format(total_7_day_exit)
            day_11_percentage_exit = "{:.1%}".format(total_11_day_exit)
            day_14_percentage_exit = "{:.1%}".format(total_14_day_exit)
            
            header_text = ["","","1 Day","4 Days","7 Days","11 Days","14 Days"]
            formatted_header_text = [Paragraph(cell, rf.tableSecondaryHeader) for cell in header_text]

            creation_data = [
                formatted_header_text,  # First row
                [Paragraph("Record Creation-Project Start", rf.tableSecondaryHeaderLeftRed), 
                Paragraph("Applies To", rf.tableSecondaryHeaderLeftRed), 
                "","", "", "", ""] ]

            creation_data.append([Paragraph("Agency Timeliness", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph(agency_1_day_percentage, rf.tableValuesStyle),Paragraph(agency_4_day_percentage, rf.tableValuesStyle),Paragraph(agency_7_day_percentage, rf.tableValuesStyle),Paragraph(agency_11_day_percentage, rf.tableValuesStyle),Paragraph(agency_14_day_percentage, rf.tableValuesStyle)])
            creation_data.append([Paragraph("Department Timeliness", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph(day_1_percentage, rf.tableValuesStyle),Paragraph(day_4_percentage, rf.tableValuesStyle),Paragraph(day_7_percentage, rf.tableValuesStyle),Paragraph(day_11_percentage, rf.tableValuesStyle),Paragraph(day_14_percentage, rf.tableValuesStyle)])
            exit_data = [formatted_header_text,
                [Paragraph("Record Creation-Project Exit", rf.tableSecondaryHeaderLeftRed), 
                Paragraph("Applies To", rf.tableSecondaryHeaderLeftRed), 
                "","", "", "", ""] ]
            
            exit_data.append([Paragraph("Agency Timeliness", rf.tableTextStyle),Paragraph("All Exited Clients", rf.tableTextStyle),Paragraph(agency_1_day_percentage_exit, rf.tableValuesStyle),Paragraph(agency_4_day_percentage_exit, rf.tableValuesStyle),Paragraph(agency_7_day_percentage_exit, rf.tableValuesStyle),Paragraph(agency_11_day_percentage_exit, rf.tableValuesStyle),Paragraph(agency_14_day_percentage_exit, rf.tableValuesStyle)])
            exit_data.append([Paragraph("Department Timeliness", rf.tableTextStyle),Paragraph("All Exited Clients", rf.tableTextStyle),Paragraph(day_1_percentage_exit, rf.tableValuesStyle),Paragraph(day_4_percentage_exit, rf.tableValuesStyle),Paragraph(day_7_percentage_exit, rf.tableValuesStyle),Paragraph(day_11_percentage_exit, rf.tableValuesStyle),Paragraph(day_14_percentage_exit, rf.tableValuesStyle)])
            
            
            filename="entry_timeliness.png"
            self.linecharts(time.percent_start_records_created_within_x_days,filename)
            start_completion_plot=get_image(filename,width=5.5*inch)
            
            filename="exit_timeliness.png"
            self.linecharts(time.percent_exit_records_created_within_x_days,filename)
            exit_completion_plot=get_image(filename,width=5.5*inch)
            
            creation_table= Table(creation_data,colWidths=[1.75*inch,1.25*inch,.9*inch,.9*inch,.9*inch,.9*inch,.9*inch],style=rf.demosTableStyle)
            exit_table= Table(exit_data,colWidths=[1.75*inch,1.25*inch,.9*inch,.9*inch,.9*inch,.9*inch,.9*inch],style=rf.demosTableStyle)
            row1demotable = Table([[creation_table],[start_completion_plot],[exit_table],[exit_completion_plot]],style=rf.demoPageTableStyle)
            
            
            page1_demographics = [[row1demotable]
                                ]
            page1_demographics_table = Table(page1_demographics, style=rf.demoPageTableStyle)
            self.elements.append(page1_demographics_table)
    
    def universal_chart(self, region=None, department=None):
                
                
                # Create the line plot
                width = 3.625
                height = 2.167
                image_dpi = 600
                fig, ax = plt.subplots(figsize=(width, height), dpi=image_dpi)
            
                start_date = self.start_date
                end_date = self.end_date
                parameters = {"start_date": start_date, "end_date": end_date}

                # Initialize a list to store the date ranges
                date_range = []

                for i in range(6):
                        current_month_start = start_date.replace(day=1) - timedelta(days=29 * i)
                        _, num_days = calendar.monthrange(current_month_start.year, current_month_start.month)
                        current_month_end = current_month_start.replace(day=num_days)
                        date_range.append((current_month_start.strftime('%Y-%m-%d'), current_month_end.strftime('%Y-%m-%d')))


                date_range.reverse()
                #print(date_range)

                month_dict = {
                    1: 'Jan',
                    2: 'Feb',
                    3: 'March',
                    4: 'April',
                    5: 'May',
                    6: 'June',
                    7: 'July',
                    8: 'Aug',
                    9: 'Sept',
                    10: 'Oct',
                    11: 'Nov',
                    12: 'Dec'
                }

                # Create empty lists to store the results and dates
                agency_pii_results=[]
                agency_universal_results=[]
                pii_results = []
                universal_results=[]
                dates = []
                pii_agency_accuracy="{:.1%}".format(dq.personal_data_quality(**parameters))
                universal_agency_accuracy="{:.1%}".format(dq.universal_data_quality(**parameters))


                for start_date, end_date in date_range:
                    parameters['start_date'] = start_date
                    parameters['end_date'] = end_date
                    agency_pii = dq.personal_data_quality(**parameters)
                    agency_universal = dq.universal_data_quality(**parameters)
                    agency_pii_results.append(agency_pii)
                    agency_universal_results.append(agency_universal)
                    date_obj = datetime.strptime(start_date, '%Y-%m-%d')
                    month_number = date_obj.month
                    year = date_obj.year
                    month_name = month_dict.get(month_number)
                    date = f'{month_name} {year}'
                    dates.append(date)
                if region:
                    title = region
                    parameters.update({"region":[region]})
                elif department:
                    title = department
                    parameters.update({"department":[department]})

                    
                for start_date, end_date in date_range:
                    parameters['start_date'] = start_date
                    parameters['end_date'] = end_date
                    pii_result=dq.personal_data_quality(**parameters)
                    pii_results.append(pii_result)
                    universal_result=dq.universal_data_quality(**parameters)
                    universal_results.append(universal_result)

                pii_result_percent=[result*100 for result in pii_results]
                universal_result_percent=[result*100 for result in universal_results]

                agency_pii_percent=[agency_result*100 for agency_result in agency_pii_results]
                agency_universal_percent=[agency_result*100 for agency_result in agency_universal_results]
                
                
                # Universal
                plt.plot(dates, universal_result_percent,marker='o',label=f'{department}',color=PATHBlue,linewidth=.75, markersize=1.5)
                plt.plot(dates, agency_universal_percent, marker='o',label='Agency',color=PATHGreen,linewidth=.75,markersize=1.5)
                plt.ylim(0,100)
                plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter())
                plt.title('Universal Data Quality Over 6 Months',fontsize=6)
                plt.xticks(fontsize=6) 
                plt.yticks(fontsize=6)  
                plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.15), fancybox=True, ncol=2, fontsize=6)
                plt.tight_layout()  # Adjust layout to prevent overlapping labels
                filename=f"{department} universal.png"
                plt.savefig(filename, bbox_inches='tight')
                plt.close() 


    def department_charts(self, region=None, department=None):
            
            def get_image(path, width=4*inch):
                img = utils.ImageReader(path)
                iw, ih = img.getSize()
                aspect = ih / float(iw)
                return Image(path, width=width, height=(width * aspect)*1.3)
            
            # Create the line plot
            width = 3.625
            height = 2.167
            image_dpi = 600
            fig, ax = plt.subplots(figsize=(width, height), dpi=image_dpi)
        
            start_date = self.start_date
            end_date = self.end_date
            parameters = {"start_date": start_date, "end_date": end_date}

            # Initialize a list to store the date ranges
            date_range = []

            for i in range(6):
                current_month_start = start_date.replace(day=1) - timedelta(days=29 * i)
                _, num_days = calendar.monthrange(current_month_start.year, current_month_start.month)
                current_month_end = current_month_start.replace(day=num_days)
                date_range.append((current_month_start.strftime('%Y-%m-%d'), current_month_end.strftime('%Y-%m-%d')))

            date_range.reverse()
            #print(date_range)
            month_dict = {
                1: 'Jan',
                2: 'Feb',
                3: 'March',
                4: 'April',
                5: 'May',
                6: 'June',
                7: 'July',
                8: 'Aug',
                9: 'Sept',
                10: 'Oct',
                11: 'Nov',
                12: 'Dec'
            }

            # Create empty lists to store the results and dates
            agency_pii_results=[]
            agency_universal_results=[]
            pii_results = []
            universal_results=[]
            dates = []
            pii_agency_accuracy="{:.1%}".format(dq.personal_data_quality(**parameters))
            universal_agency_accuracy="{:.1%}".format(dq.universal_data_quality(**parameters))


            for start_date, end_date in date_range:
                parameters['start_date'] = start_date
                parameters['end_date'] = end_date

                agency_pii = dq.personal_data_quality(**parameters)
                agency_universal = dq.universal_data_quality(**parameters)
                agency_pii_results.append(agency_pii)
                agency_universal_results.append(agency_universal)
                date_obj = datetime.strptime(start_date, '%Y-%m-%d')
                month_number = date_obj.month
                year = date_obj.year
                month_name = month_dict.get(month_number)
                date = f'{month_name} {year}'
                dates.append(date)
            if region:
                title = region
                parameters.update({"region":[region]})
            elif department:
                title = department
                parameters.update({"department":[department]})

                
            for start_date, end_date in date_range:
                parameters['start_date'] = start_date
                parameters['end_date'] = end_date
                pii_result=dq.personal_data_quality(**parameters)
                pii_results.append(pii_result)
                universal_result=dq.universal_data_quality(**parameters)
                universal_results.append(universal_result)

            pii_result_percent=[result*100 for result in pii_results]
            universal_result_percent=[result*100 for result in universal_results]

            agency_pii_percent=[agency_result*100 for agency_result in agency_pii_results]
            agency_universal_percent=[agency_result*100 for agency_result in agency_universal_results]

            # PII
            plt.plot(dates, pii_result_percent,marker='o',label=f'{department}',color=PATHBlue,linewidth=.75, markersize=1.5)
            plt.plot(dates, agency_pii_percent, marker='o',label='Agency',color=PATHGreen,linewidth=.75,markersize=1.5)
            plt.ylim(0,100)
            plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter())
            plt.title('Personal Data Quality Over 6 Months',fontsize=6)
            plt.xticks(fontsize=6) 
            plt.yticks(fontsize=6)  
            plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.15), fancybox=True, ncol=2, fontsize=6)
            plt.tight_layout()  # Adjust layout to prevent overlapping labels
            filename=f"{department} PII.png"
            plt.savefig(filename, bbox_inches='tight')
            plt.close() 
            
            header_text = ["Personal Identifiable Information Data Quality","%"]
            formatted_header_text = [Paragraph(cell, rf.tableSecondaryHeader) for cell in header_text]
            pii_data = [formatted_header_text]
            pii_dept_accuracy="{:.1%}".format(dq.personal_data_quality(**parameters))
            pii_data.append([Paragraph("Agency",rf.tableTextStyle),Paragraph(pii_agency_accuracy,rf.tableValuesStyle)])
            pii_data.append([Paragraph(f"{department}",rf.tableTextStyle),Paragraph(pii_dept_accuracy,rf.tableValuesStyle)])
            pii_table = Table(pii_data, colWidths=[3*inch,.50*inch],style=rf.demosTableStyle)  
            pii_plot=get_image(filename,width=3.6*inch)
            
            
            header_text = ["Universal Information Information Data Quality","%"]
            formatted_header_text = [Paragraph(cell, rf.tableSecondaryHeader) for cell in header_text]
            universal_data = [formatted_header_text]
            universal_dept_accuracy="{:.1%}".format(dq.universal_data_quality(**parameters))
            universal_data.append([Paragraph("Agency",rf.tableTextStyle),Paragraph(universal_agency_accuracy,rf.tableValuesStyle)])
            universal_data.append([Paragraph(f"{department}",rf.tableTextStyle),Paragraph(universal_dept_accuracy,rf.tableValuesStyle)])
            universal_table = Table(universal_data, colWidths=[3*inch,.50*inch],style=rf.demosTableStyle)  
            self.universal_chart(department=department)
            filename=f"{department} universal.png"
            universal_plot=get_image(filename,width=3.6*inch)
            
            row1demotable = Table([[pii_table,universal_table],[pii_plot,universal_plot]],style=rf.demoPageTableStyle)

            page1_demographics = [[row1demotable]
                                ]
            page1_demographics_table = Table(page1_demographics, style=rf.demoPageTableStyle)
            self.elements.append(page1_demographics_table)

    
    def program_dataquality(self, region=None, department=None,program_type=None,program_id=None):
            
            parameters = ({"start_date": self.start_date, "end_date": self.end_date,"department":[department]})
            name=dq.Name()
            ssn=dq.SSN()
            dob=dq.DOB()
            race=dq.Race()
            gender=dq.Gender()
            veteran=dq.Veteran()
            start=dq.StartDate()
            hoh=dq.HOH()
            coc=dq.Location()
            disability=dq.Disabling()
            destination=dq.Destination()
            income=dq.Income()
            
            dept_count="{:,}".format(dq.active_clients(**parameters))
            
            dept_name_accuracy = name.name_total_accuracy(**parameters)            
            dept_name_percentage = "{:.1%}".format(dept_name_accuracy)
            
            dept_ssn_accuracy = ssn.ssn_total_accuracy(**parameters)
            dept_ssn_percentage = "{:.1%}".format(dept_ssn_accuracy)
            
            dept_dob_accuracy = dob.dob_total_accuracy(**parameters)
            dept_dob_percentage = "{:.1%}".format(dept_dob_accuracy)

            dept_race_accuracy = race.race_total_accuracy(**parameters)
            dept_race_percentage = "{:.1%}".format(dept_race_accuracy)

            dept_gender_accuracy = gender.gender_total_accuracy(**parameters)
            dept_gender_percentage = "{:.1%}".format(dept_gender_accuracy)

            dept_veteran_accuracy = veteran.veteran_total_accuracy(**parameters)
            dept_veteran_percentage = "{:.1%}".format(dept_veteran_accuracy)
            
            dept_start_accuracy = start.start_date_data_accuracy(**parameters)
            dept_start_percentage = "{:.1%}".format(dept_start_accuracy)
            
            dept_hoh_accuracy = hoh.HoH_total_accuracy(**parameters)
            dept_hoh_percentage = "{:.1%}".format(dept_hoh_accuracy)
            
            dept_coc_accuracy = coc.enrollment_coc_total_accuracy(**parameters)
            dept_coc_percentage = "{:.1%}".format(dept_coc_accuracy)
            
            dept_disability_accuracy = disability.disabling_condition_total_accuracy(**parameters)
            dept_disability_percentage = "{:.1%}".format(dept_disability_accuracy)

            dept_destination_accuracy = destination.destination_total_accuracy(**parameters)
            dept_destination_percentage = "{:.1%}".format(dept_destination_accuracy)
            
            dept_start_income_accuracy = income.starting_income_total_accuracy(**parameters)
            dept_start_income_percentage = "{:.1%}".format(dept_start_income_accuracy)
            
            dept_annual_income_accuracy=income.annual_income_total_accuracy(**parameters)
            dept_annual_income_percentage="{:.1%}".format(dept_annual_income_accuracy)
            
            dept_exit_income_accuracy = income.exiting_income_total_accuracy(**parameters)
            dept_exit_income_percentage = "{:.1%}".format(dept_exit_income_accuracy)
            

            parameters.update({"program_id":[program_id]})

            program_count="{:,}".format(dq.active_clients(**parameters))
            
            total_name_accuracy = name.name_total_accuracy(**parameters)
            client_name_refused = name.name_client_refused_doesnt_know(**parameters)
            missing_name = name.name_missing(**parameters)
            issues_name = name.name_data_accuracy(**parameters)
            
            total_name_percentage = "{:.1%}".format(total_name_accuracy)
            refused_name_percentage = "{:.1%}".format(1-client_name_refused)
            missing_name_percentage = "{:.1%}".format(1-missing_name)
            issues_name_percentage = "{:.1%}".format(1-issues_name)
            
            total_ssn_accuracy = ssn.ssn_total_accuracy(**parameters)
            client_refused_ssn = ssn.ssn_client_refused_doesnt_know(**parameters)
            missing_ssn = ssn.ssn_missing(**parameters)
            issues_ssn = ssn.ssn_data_accuracy(**parameters)
            
            total_ssn_percentage = "{:.1%}".format(total_ssn_accuracy)
            refused_ssn_percentage = "{:.1%}".format(1-client_refused_ssn)
            missing_ssn_percentage = "{:.1%}".format(1-missing_ssn)
            issues_ssn_percentage = "{:.1%}".format(1-issues_ssn)
            
            total_dob_accuracy = dob.dob_total_accuracy(**parameters)
            client_refused_dob = dob.dob_client_refused_doesnt_know(**parameters)
            missing_dob = dob.dob_missing(**parameters)
            issues_dob = dob.dob_data_accuracy(**parameters)
            
            total_dob_percentage = "{:.1%}".format(total_dob_accuracy)
            refused_dob_percentage = "{:.1%}".format(1-client_refused_dob)
            missing_dob_percentage = "{:.1%}".format(1-missing_dob)
            issues_dob_percentage = "{:.1%}".format(1-issues_dob)
            
            total_race_accuracy = race.race_total_accuracy(**parameters)
            client_refused_race = race.race_client_refused_doesnt_know(**parameters)
            missing_race = race.race_missing(**parameters)
            
            total_race_percentage = "{:.1%}".format(total_race_accuracy)
            refused_percentage_race = "{:.1%}".format(1-client_refused_race)
            missing_percentage_race = "{:.1%}".format(1-missing_race)
            
            total_gender_accuracy = gender.gender_total_accuracy(**parameters)
            client_refused_gender = gender.gender_client_refused_doesnt_know(**parameters)
            missing_gender = gender.gender_missing(**parameters)
            
            total_gender_percentage = "{:.1%}".format(total_gender_accuracy)
            refused_gender_percentage = "{:.1%}".format(1-client_refused_gender)
            missing_gender_percentage = "{:.1%}".format(1-missing_gender)
            
            total_veteran_accuracy = veteran.veteran_total_accuracy(**parameters)
            client_refused_veteran = veteran.veteran_client_refused_doesnt_know(**parameters)
            missing_veteran = veteran.veteran_missing(**parameters)
            issues_veteran = veteran.veteran_data_accuracy(**parameters)
            
            total_veteran_percentage = "{:.1%}".format(total_veteran_accuracy)
            refused_veteran_percentage = "{:.1%}".format(1-client_refused_veteran)
            missing_veteran_percentage = "{:.1%}".format(1-missing_veteran)
            issues_veteran_percentage = "{:.1%}".format(1-issues_veteran)
            
            total_start_accuracy = start.start_date_data_accuracy(**parameters)
            total_start_percentage = "{:.1%}".format(total_start_accuracy)
            
            total_hoh_accuracy = hoh.HoH_total_accuracy(**parameters)
            missing_hoh = hoh.HoH_missing(**parameters)
            issues_hoh = hoh.HoH_data_accuracy(**parameters)

            total_hoh_percentage = "{:.1%}".format(total_hoh_accuracy)
            missing_hoh_percentage = "{:.1%}".format(1-missing_hoh)
            issues_hoh_percentage = "{:.1%}".format(1-issues_hoh)
            
            total_coc_accuracy = coc.enrollment_coc_total_accuracy(**parameters)
            missing_coc = coc.enrollment_coc_missing(**parameters)
            issues_coc = coc.enrollment_coc_data_accuracy(**parameters)

            total_coc_percentage = "{:.1%}".format(total_coc_accuracy)
            missing_coc_percentage = "{:.1%}".format(1-missing_coc)
            issues_coc_percentage = "{:.1%}".format(1-issues_coc)
            
            total_disability_accuracy = disability.disabling_condition_total_accuracy(**parameters)
            client_refused_disability= disability.disabling_condition_client_refused(**parameters)
            missing_disability = disability.disabling_condition_missing(**parameters)
            issues_disability = disability.disabling_condition_data_accuracy(**parameters)

            total_disability_percentage = "{:.1%}".format(total_disability_accuracy)
            refused_disability_percentage = "{:.1%}".format(1-client_refused_disability)
            missing_disability_percentage = "{:.1%}".format(1-missing_disability)
            issues_disability_percentage = "{:.1%}".format(1-issues_disability)
            
            total_destination_accuracy = destination.destination_total_accuracy(**parameters)
            client_refused_destination = destination.destination_client_refused_doesnt_know(**parameters)
            missing_destination = destination.destination_missing(**parameters)
            
    
            total_destination_percentage = "{:.1%}".format(total_destination_accuracy)
            refused_destination_percentage = "{:.1%}".format(1-client_refused_destination)
            missing_destination_percentage = "{:.1%}".format(1-missing_destination)
            total_leavers=destination.number_leavers(**parameters)
            
            total_start_income_accuracy = income.starting_income_total_accuracy(**parameters)
            client_refused_start_income= income.starting_income_client_refused(**parameters)
            missing_start_income = income.starting_income_missing(**parameters)
            issues_start_income = income.starting_income_data_accuracy(**parameters)

            total_start_income_percentage = "{:.1%}".format(total_start_income_accuracy)
            refused_start_income_percentage = "{:.1%}".format(1-client_refused_start_income)
            missing_start_income_percentage = "{:.1%}".format(1-missing_start_income)
            issues_start_income_percentage = "{:.1%}".format(1-issues_start_income)
            
            total_annual_income_accuracy = income.annual_income_total_accuracy(**parameters)
            client_refused_annual_income= income.annual_income_client_refused(**parameters)
            missing_annual_income = income.annual_income_missing(**parameters)
            issues_annual_income = income.annual_income_data_accuracy(**parameters)

            total_annual_income_percentage = "{:.1%}".format(total_annual_income_accuracy)
            refused_annual_income_percentage = "{:.1%}".format(1-client_refused_annual_income)
            missing_annual_income_percentage = "{:.1%}".format(1-missing_annual_income)
            issues_annual_income_percentage = "{:.1%}".format(1-issues_annual_income)
            
            total_exit_income_accuracy = income.exiting_income_total_accuracy(**parameters)
            client_refused_exit_income= income.exiting_income_client_refused(**parameters)
            missing_exit_income = income.exiting_income_client_missing(**parameters)
            issues_exit_income = income.exiting_income_data_accuracy(**parameters)

            total_exit_income_percentage = "{:.1%}".format(total_exit_income_accuracy)
            refused_exit_income_percentage = "{:.1%}".format(1-client_refused_exit_income)
            missing_exit_income_percentage = "{:.1%}".format(1-missing_exit_income)
            issues_exit_income_percentage = "{:.1%}".format(1-issues_exit_income)
            if department== 'Permanent Supportive Services':
                department='PSS'
                
            header_text = [f"Program Clients: {program_count}", f"{department} Clients: {dept_count}","Agency Goal", "Depart. Accuracy","Program Accuracy", "Client Refused", "Missing", "Data Issues"]
            formatted_header_text = [Paragraph(cell, rf.tableSecondaryHeader) for cell in header_text]

            # Create two rows of headers
            personal_data = [
                formatted_header_text,  # First row
                [Paragraph("Personal Data Quality", rf.tableSecondaryHeaderLeftBlue), 
                Paragraph("Applies To", rf.tableSecondaryHeaderLeftBlue), 
                "","", "","", "", ""] ] # Second row
            if program_count=="0":
                personal_data.append([Paragraph("Name", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_name_percentage, rf.tableValuesStylegray),Paragraph("No Clients", rf.tableValuesStylegray),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
                personal_data.append([Paragraph("Social Security Number", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_ssn_percentage, rf.tableValuesStylegray),Paragraph("No Clients", rf.tableValuesStylegray),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
                personal_data.append([Paragraph("Date of Birth", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_dob_percentage, rf.tableValuesStylegray),Paragraph("No Clients", rf.tableValuesStylegray),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
                personal_data.append([Paragraph("Race & Ethnicity", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_race_percentage, rf.tableValuesStylegray),Paragraph("No Clients", rf.tableValuesStylegray),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
                personal_data.append([Paragraph("Gender", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_gender_percentage, rf.tableValuesStylegray),Paragraph("No Clients", rf.tableValuesStylegray),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])

                universal_data = [ # First row
                    [Paragraph("Universal Data Quality", rf.tableSecondaryHeaderLeftPurple), 
                    Paragraph("Applies To", rf.tableSecondaryHeaderLeftPurple), 
                    "","", "", "","", ""] ] # Second row
                
                universal_data.append([Paragraph("Veteran Status", rf.tableTextStyle),Paragraph("All Adult Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(dept_veteran_percentage, rf.tableValuesStylegray),Paragraph("No Clients", rf.tableValuesStylegray),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
                universal_data.append([Paragraph("Project Start Date", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_start_percentage, rf.tableValuesStylegray),Paragraph("No Clients", rf.tableValuesStylegray),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
                universal_data.append([Paragraph("Head of Household", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(dept_hoh_percentage, rf.tableValuesStylegray),Paragraph("No Clients", rf.tableValuesStylegray),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
                universal_data.append([Paragraph("Continuum of Care", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_coc_percentage, rf.tableValuesStylegray),Paragraph("No Clients", rf.tableValuesStylegray),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
                universal_data.append([Paragraph("Disability Status", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(dept_disability_percentage, rf.tableValuesStylegray),Paragraph("No Clients", rf.tableValuesStylegray),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])

                income_data = [ # First row
                    [Paragraph("Income & Housing Data Quality", rf.tableSecondaryHeaderLeftGreen), 
                    Paragraph("Applies To", rf.tableSecondaryHeaderLeftGreen), 
                    "","", "","", "", ""] ] # Second row
                
                income_data.append([Paragraph("Exit Destination", rf.tableTextStyle),Paragraph("All Exited Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_destination_percentage, rf.tableValuesStylegray),Paragraph('No Exits', rf.tableValuesStylegray),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
                income_data.append([Paragraph("Income at Entry", rf.tableTextStyle),Paragraph("All Adult Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(dept_start_income_percentage, rf.tableValuesStylegray),Paragraph("No Clients", rf.tableValuesStylegray),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.ComingSoonStyle),Paragraph(" - ", rf.ComingSoonStyle)])
                income_data.append([Paragraph("Income at Annual Assessment", rf.tableTextStyle),Paragraph("All Adult Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(dept_annual_income_percentage, rf.tableValuesStylegray),Paragraph("No Clients", rf.tableValuesStylegray),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.ComingSoonStyle),Paragraph(" - ", rf.ComingSoonStyle)])
                income_data.append([Paragraph("Income at Exit", rf.tableTextStyle),Paragraph("All Adult Exited Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(dept_exit_income_percentage, rf.tableValuesStylegray),Paragraph("No Exits", rf.tableValuesStylegray),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.ComingSoonStyle),Paragraph(" - ", rf.ComingSoonStyle)])

                personal_table = Table(personal_data,colWidths=[1.75*inch,1.15*inch,.8*inch,.8*inch,.8*inch,.7*inch,.7*inch,.7*inch],style=rf.demosTableStyle)
                universal_table=Table(universal_data,colWidths=[1.75*inch,1.15*inch,.8*inch,.8*inch,.8*inch,.7*inch,.7*inch,.7*inch],style=rf.demosTableStylewhite)
                income_table=Table(income_data,colWidths=[1.75*inch,1.15*inch,.8*inch,.8*inch,.8*inch,.7*inch,.7*inch,.7*inch],style=rf.demosTableStylewhite)
                row1demotable = Table([[personal_table],[universal_table],[income_table]],style=rf.demoPageTableStyle)
                
                page1_demographics = [[row1demotable]
                                    ]
                page1_demographics_table = Table(page1_demographics, style=rf.demoPageTableStyle)
                self.elements.append(page1_demographics_table)
            elif program_type=='Outreach Services':   
                personal_data.append([Paragraph("Name", rf.tableTextStyle),Paragraph("Enagaged Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_name_percentage, rf.tableValuesStylegray),Paragraph(total_name_percentage, rf.nintyfive_cellColor_gradient(total_name_percentage)),Paragraph(refused_name_percentage, rf.tableValuesStyle),Paragraph(missing_name_percentage, rf.tableValuesStyle),Paragraph(issues_name_percentage, rf.tableValuesStyle)])
                personal_data.append([Paragraph("Social Security Number", rf.tableTextStyle),Paragraph("Enagaged Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_ssn_percentage, rf.tableValuesStylegray),Paragraph(total_ssn_percentage, rf.nintyfive_cellColor_gradient(total_ssn_percentage),),Paragraph(refused_ssn_percentage, rf.tableValuesStyle),Paragraph(missing_ssn_percentage, rf.tableValuesStyle),Paragraph(issues_ssn_percentage, rf.tableValuesStyle)])
                personal_data.append([Paragraph("Date of Birth", rf.tableTextStyle),Paragraph("Enagaged Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_dob_percentage, rf.tableValuesStylegray),Paragraph(total_dob_percentage, rf.nintyfive_cellColor_gradient(total_dob_percentage),),Paragraph(refused_dob_percentage, rf.tableValuesStyle),Paragraph(missing_dob_percentage, rf.tableValuesStyle),Paragraph(issues_dob_percentage, rf.tableValuesStyle)])
                personal_data.append([Paragraph("Race & Ethnicity", rf.tableTextStyle),Paragraph("Enagaged Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_race_percentage, rf.tableValuesStylegray),Paragraph(total_race_percentage, rf.nintyfive_cellColor_gradient(total_race_percentage),),Paragraph(refused_percentage_race, rf.tableValuesStyle),Paragraph(missing_percentage_race, rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
                personal_data.append([Paragraph("Gender", rf.tableTextStyle),Paragraph("Enagaged Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_gender_percentage, rf.tableValuesStylegray),Paragraph(total_gender_percentage, rf.nintyfive_cellColor_gradient(total_gender_percentage),),Paragraph(refused_gender_percentage, rf.tableValuesStyle),Paragraph(missing_gender_percentage, rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])

                universal_data = [ # First row
                    [Paragraph("Universal Data Quality", rf.tableSecondaryHeaderLeftPurple), 
                    Paragraph("Applies To", rf.tableSecondaryHeaderLeftPurple), 
                    "","", "", "", ""] ] # Second row
                
                universal_data.append([Paragraph("Veteran Status", rf.tableTextStyle),Paragraph("Engaged Adult Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(dept_veteran_percentage, rf.tableValuesStylegray),Paragraph(total_veteran_percentage, rf.ninty_cellColor_gradient(total_veteran_percentage)),Paragraph(refused_veteran_percentage, rf.tableValuesStyle),Paragraph(missing_veteran_percentage, rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
                universal_data.append([Paragraph("Project Start Date", rf.tableTextStyle),Paragraph("Enagaged Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_start_percentage, rf.tableValuesStylegray),Paragraph(total_start_percentage, rf.nintyfive_cellColor_gradient(total_start_percentage)),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
                universal_data.append([Paragraph("Head of Household", rf.tableTextStyle),Paragraph("Enagaged Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(dept_hoh_percentage, rf.tableValuesStylegray),Paragraph(total_hoh_percentage, rf.ninty_cellColor_gradient(total_hoh_percentage)),Paragraph(" - ", rf.tableValuesStyle),Paragraph(missing_hoh_percentage, rf.tableValuesStyle),Paragraph(issues_hoh_percentage, rf.tableValuesStyle)])
                universal_data.append([Paragraph("Continuum of Care", rf.tableTextStyle),Paragraph("Enagaged Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_coc_percentage, rf.tableValuesStylegray),Paragraph(total_coc_percentage, rf.nintyfive_cellColor_gradient(total_coc_percentage)),Paragraph(" - ", rf.tableValuesStyle),Paragraph(missing_coc_percentage, rf.tableValuesStyle),Paragraph(issues_coc_percentage, rf.tableValuesStyle)])
                universal_data.append([Paragraph("Disability Status", rf.tableTextStyle),Paragraph("Enagaged Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(dept_disability_percentage, rf.tableValuesStylegray),Paragraph(total_disability_percentage, rf.ninty_cellColor_gradient(total_disability_percentage)),Paragraph(refused_disability_percentage, rf.tableValuesStyle),Paragraph(missing_disability_percentage, rf.tableValuesStyle),Paragraph(issues_disability_percentage, rf.tableValuesStyle)])

                income_data = [ # First row
                    [Paragraph("Income & Housing Data Quality", rf.tableSecondaryHeaderLeftGreen), 
                    Paragraph("Applies To", rf.tableSecondaryHeaderLeftGreen), 
                    "","", "","", "", ""] ] # Second row
                if total_leavers >0:
                    income_data.append([Paragraph("Exit Destination", rf.tableTextStyle),Paragraph("Engaged Exited Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_destination_percentage, rf.tableValuesStylegray),Paragraph(total_destination_percentage, rf.nintyfive_cellColor_gradient(total_destination_percentage)),Paragraph(refused_destination_percentage, rf.tableValuesStyle),Paragraph(missing_destination_percentage, rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
                    income_data.append([Paragraph("Income at Entry", rf.tableTextStyle),Paragraph("Engaged Adult Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(dept_start_income_percentage, rf.tableValuesStylegray),Paragraph(total_start_income_percentage, rf.ninty_cellColor_gradient(total_start_income_percentage)),Paragraph(refused_start_income_percentage, rf.tableValuesStyle),Paragraph(missing_start_income_percentage, rf.tableValuesStyle),Paragraph(issues_start_income_percentage, rf.tableValuesStyle)])
                    income_data.append([Paragraph("Income at Annual Assessment", rf.tableTextStyle),Paragraph("Engaged Adult Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(dept_annual_income_percentage, rf.tableValuesStylegray),Paragraph(total_annual_income_percentage, rf.ninty_cellColor_gradient(total_annual_income_percentage)),Paragraph(refused_annual_income_percentage, rf.tableValuesStyle),Paragraph(missing_annual_income_percentage, rf.tableValuesStyle),Paragraph(issues_annual_income_percentage, rf.tableValuesStyle)])
                    income_data.append([Paragraph("Income at Exit", rf.tableTextStyle),Paragraph("Engaged Adult Exited Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(dept_exit_income_percentage, rf.tableValuesStylegray),Paragraph(total_exit_income_percentage, rf.ninty_cellColor_gradient(total_exit_income_percentage)),Paragraph(refused_exit_income_percentage, rf.tableValuesStyle),Paragraph(missing_exit_income_percentage, rf.tableValuesStyle),Paragraph(issues_exit_income_percentage, rf.tableValuesStyle)])
                else:
                    income_data.append([Paragraph("Exit Destination", rf.tableTextStyle),Paragraph("Engaged Exited Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_destination_percentage, rf.tableValuesStylegray),Paragraph('No Exits', rf.tableValuesStylegray),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
                    income_data.append([Paragraph("Income at Entry", rf.tableTextStyle),Paragraph("Engaged Adult Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(dept_start_income_percentage, rf.tableValuesStylegray),Paragraph(total_start_income_percentage, rf.ninty_cellColor_gradient(total_start_income_percentage)),Paragraph(refused_start_income_percentage, rf.tableValuesStyle),Paragraph(missing_start_income_percentage, rf.tableValuesStyle),Paragraph(issues_start_income_percentage, rf.tableValuesStyle)])
                    income_data.append([Paragraph("Income at Annual Assessment", rf.tableTextStyle),Paragraph("Engaged Adult Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(dept_annual_income_percentage, rf.tableValuesStylegray),Paragraph(total_annual_income_percentage, rf.ninty_cellColor_gradient(total_annual_income_percentage)),Paragraph(refused_annual_income_percentage, rf.tableValuesStyle),Paragraph(missing_annual_income_percentage, rf.tableValuesStyle),Paragraph(issues_annual_income_percentage, rf.tableValuesStyle)])
                    income_data.append([Paragraph("Income at Exit", rf.tableTextStyle),Paragraph("Engaged Adult Exited Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(dept_exit_income_percentage, rf.tableValuesStylegray),Paragraph('No Exits', rf.tableValuesStylegray),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
                    
                personal_table = Table(personal_data,colWidths=[1.75*inch,1.15*inch,.8*inch,.8*inch,.8*inch,.7*inch,.7*inch,.7*inch],style=rf.demosTableStyle)
                universal_table=Table(universal_data,colWidths=[1.75*inch,1.15*inch,.8*inch,.8*inch,.8*inch,.7*inch,.7*inch,.7*inch],style=rf.demosTableStylewhite)
                income_table=Table(income_data,colWidths=[1.75*inch,1.15*inch,.8*inch,.8*inch,.8*inch,.7*inch,.7*inch,.7*inch],style=rf.demosTableStylewhite)
                row1demotable = Table([[personal_table],[universal_table],[income_table]],style=rf.demoPageTableStyle)
                
                page1_demographics = [[row1demotable]
                                    ]
                page1_demographics_table = Table(page1_demographics, style=rf.demoPageTableStyle)
                self.elements.append(page1_demographics_table)
            
            else:   
                personal_data.append([Paragraph("Name", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_name_percentage, rf.tableValuesStylegray),Paragraph(total_name_percentage, rf.nintyfive_cellColor_gradient(total_name_percentage)),Paragraph(refused_name_percentage, rf.tableValuesStyle),Paragraph(missing_name_percentage, rf.tableValuesStyle),Paragraph(issues_name_percentage, rf.tableValuesStyle)])
                personal_data.append([Paragraph("Social Security Number", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_ssn_percentage, rf.tableValuesStylegray),Paragraph(total_ssn_percentage, rf.nintyfive_cellColor_gradient(total_ssn_percentage),),Paragraph(refused_ssn_percentage, rf.tableValuesStyle),Paragraph(missing_ssn_percentage, rf.tableValuesStyle),Paragraph(issues_ssn_percentage, rf.tableValuesStyle)])
                personal_data.append([Paragraph("Date of Birth", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_dob_percentage, rf.tableValuesStylegray),Paragraph(total_dob_percentage, rf.nintyfive_cellColor_gradient(total_dob_percentage),),Paragraph(refused_dob_percentage, rf.tableValuesStyle),Paragraph(missing_dob_percentage, rf.tableValuesStyle),Paragraph(issues_dob_percentage, rf.tableValuesStyle)])
                personal_data.append([Paragraph("Race & Ethnicity", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_race_percentage, rf.tableValuesStylegray),Paragraph(total_race_percentage, rf.nintyfive_cellColor_gradient(total_race_percentage),),Paragraph(refused_percentage_race, rf.tableValuesStyle),Paragraph(missing_percentage_race, rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
                personal_data.append([Paragraph("Gender", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_gender_percentage, rf.tableValuesStylegray),Paragraph(total_gender_percentage, rf.nintyfive_cellColor_gradient(total_gender_percentage),),Paragraph(refused_gender_percentage, rf.tableValuesStyle),Paragraph(missing_gender_percentage, rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])

                universal_data = [ # First row
                    [Paragraph("Universal Data Quality", rf.tableSecondaryHeaderLeftPurple), 
                    Paragraph("Applies To", rf.tableSecondaryHeaderLeftPurple), 
                    "","", "", "", ""] ] # Second row
                
                universal_data.append([Paragraph("Veteran Status", rf.tableTextStyle),Paragraph("All Adult Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(dept_veteran_percentage, rf.tableValuesStylegray),Paragraph(total_veteran_percentage, rf.ninty_cellColor_gradient(total_veteran_percentage)),Paragraph(refused_veteran_percentage, rf.tableValuesStyle),Paragraph(missing_veteran_percentage, rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
                universal_data.append([Paragraph("Project Start Date", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_start_percentage, rf.tableValuesStylegray),Paragraph(total_start_percentage, rf.nintyfive_cellColor_gradient(total_start_percentage)),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
                universal_data.append([Paragraph("Head of Household", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(dept_hoh_percentage, rf.tableValuesStylegray),Paragraph(total_hoh_percentage, rf.ninty_cellColor_gradient(total_hoh_percentage)),Paragraph(" - ", rf.tableValuesStyle),Paragraph(missing_hoh_percentage, rf.tableValuesStyle),Paragraph(issues_hoh_percentage, rf.tableValuesStyle)])
                universal_data.append([Paragraph("Continuum of Care", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_coc_percentage, rf.tableValuesStylegray),Paragraph(total_coc_percentage, rf.nintyfive_cellColor_gradient(total_coc_percentage)),Paragraph(" - ", rf.tableValuesStyle),Paragraph(missing_coc_percentage, rf.tableValuesStyle),Paragraph(issues_coc_percentage, rf.tableValuesStyle)])
                universal_data.append([Paragraph("Disability Status", rf.tableTextStyle),Paragraph("All Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(dept_disability_percentage, rf.tableValuesStylegray),Paragraph(total_disability_percentage, rf.ninty_cellColor_gradient(total_disability_percentage)),Paragraph(refused_disability_percentage, rf.tableValuesStyle),Paragraph(missing_disability_percentage, rf.tableValuesStyle),Paragraph(issues_disability_percentage, rf.tableValuesStyle)])

                income_data = [ # First row
                    [Paragraph("Income & Housing Data Quality", rf.tableSecondaryHeaderLeftGreen), 
                    Paragraph("Applies To", rf.tableSecondaryHeaderLeftGreen), 
                    "","", "","", "", ""] ] # Second row
                if total_leavers >0:
                    income_data.append([Paragraph("Exit Destination", rf.tableTextStyle),Paragraph("All Exited Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_destination_percentage, rf.tableValuesStylegray),Paragraph(total_destination_percentage, rf.nintyfive_cellColor_gradient(total_destination_percentage)),Paragraph(refused_destination_percentage, rf.tableValuesStyle),Paragraph(missing_destination_percentage, rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
                    income_data.append([Paragraph("Income at Entry", rf.tableTextStyle),Paragraph("All Adult Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(dept_start_income_percentage, rf.tableValuesStylegray),Paragraph(total_start_income_percentage, rf.ninty_cellColor_gradient(total_start_income_percentage)),Paragraph(refused_start_income_percentage, rf.tableValuesStyle),Paragraph(missing_start_income_percentage, rf.tableValuesStyle),Paragraph(issues_start_income_percentage, rf.tableValuesStyle)])
                    income_data.append([Paragraph("Income at Annual Assessment", rf.tableTextStyle),Paragraph("All Adult Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(dept_annual_income_percentage, rf.tableValuesStylegray),Paragraph(total_annual_income_percentage, rf.ninty_cellColor_gradient(total_annual_income_percentage)),Paragraph(refused_annual_income_percentage, rf.tableValuesStyle),Paragraph(missing_annual_income_percentage, rf.tableValuesStyle),Paragraph(issues_annual_income_percentage, rf.tableValuesStyle)])
                    income_data.append([Paragraph("Income at Exit", rf.tableTextStyle),Paragraph("All Adult Exited Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(dept_exit_income_percentage, rf.tableValuesStylegray),Paragraph(total_exit_income_percentage, rf.ninty_cellColor_gradient(total_exit_income_percentage)),Paragraph(refused_exit_income_percentage, rf.tableValuesStyle),Paragraph(missing_exit_income_percentage, rf.tableValuesStyle),Paragraph(issues_exit_income_percentage, rf.tableValuesStyle)])
                else:
                    income_data.append([Paragraph("Exit Destination", rf.tableTextStyle),Paragraph("All Exited Clients", rf.tableTextStyle),Paragraph("95.0%", rf.tableValuesStyle),Paragraph(dept_destination_percentage, rf.tableValuesStylegray),Paragraph('No Exits', rf.tableValuesStylegray),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
                    income_data.append([Paragraph("Income at Entry", rf.tableTextStyle),Paragraph("All Adult Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(dept_start_income_percentage, rf.tableValuesStylegray),Paragraph(total_start_income_percentage, rf.ninty_cellColor_gradient(total_start_income_percentage)),Paragraph(refused_start_income_percentage, rf.tableValuesStyle),Paragraph(missing_start_income_percentage, rf.tableValuesStyle),Paragraph(issues_start_income_percentage, rf.tableValuesStyle)])
                    income_data.append([Paragraph("Income at Annual Assessment", rf.tableTextStyle),Paragraph("All Adult Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(dept_annual_income_percentage, rf.tableValuesStylegray),Paragraph(total_annual_income_percentage, rf.ninty_cellColor_gradient(total_annual_income_percentage)),Paragraph(refused_annual_income_percentage, rf.tableValuesStyle),Paragraph(missing_annual_income_percentage, rf.tableValuesStyle),Paragraph(issues_annual_income_percentage, rf.tableValuesStyle)])
                    income_data.append([Paragraph("Income at Exit", rf.tableTextStyle),Paragraph("All Adult Exited Clients", rf.tableTextStyle),Paragraph("90.0%", rf.tableValuesStyle),Paragraph(dept_exit_income_percentage, rf.tableValuesStylegray),Paragraph('No Exits', rf.tableValuesStylegray),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle),Paragraph(" - ", rf.tableValuesStyle)])
                personal_table = Table(personal_data,colWidths=[1.75*inch,1.15*inch,.8*inch,.8*inch,.8*inch,.7*inch,.7*inch,.7*inch],style=rf.demosTableStyle)
                universal_table=Table(universal_data,colWidths=[1.75*inch,1.15*inch,.8*inch,.8*inch,.8*inch,.7*inch,.7*inch,.7*inch],style=rf.demosTableStylewhite)
                income_table=Table(income_data,colWidths=[1.75*inch,1.15*inch,.8*inch,.8*inch,.8*inch,.7*inch,.7*inch,.7*inch],style=rf.demosTableStylewhite)
                row1demotable = Table([[personal_table],[universal_table],[income_table]],style=rf.demoPageTableStyle)
                
                page1_demographics = [[row1demotable]
                                    ]
                page1_demographics_table = Table(page1_demographics, style=rf.demoPageTableStyle)
                self.elements.append(page1_demographics_table)
                income_percentage_note = "*Subtotals for income quality categories may add up to over a 100% due to some enrollments having multiple income entries that get flagged for differing quality concerns**"
                self.elements.append(Paragraph(income_percentage_note, rf.noteStyle2))
    
    def program_timeliness(self, region=None, department=None,program_id=None):
            parameters = ({"start_date": self.start_date, "end_date": self.end_date})
            
            if region:
                title = region
                parameters.update({"region":[region]})
            elif department:
                title = department
                parameters.update({"department":[department]})
            elif program_id:
                parameters.update({"program_id":[program_id]})


            def get_image(path, width=4*inch):
                img = utils.ImageReader(path)
                iw, ih = img.getSize()
                aspect = ih / float(iw)
                return Image(path, width=width, height=(width * aspect)*.8)

            day_1_threshold="{:.1%}".format(0.80)
            day_4_threshold="{:.1%}".format(0.85)
            day_7_threshold="{:.1%}".format(0.90)
            day_11_threshold="{:.1%}".format(0.95)
            day_14_threshold="{:.1%}".format(1.0)
            
            time=dq.Timeliness()
            destination=dq.Destination()
            ##time-start
            total_1_day = time.percent_start_records_created_within_x_days(**parameters, days=1)
            total_4_day = time.percent_start_records_created_within_x_days(**parameters, days=4)
            total_7_day = time.percent_start_records_created_within_x_days(**parameters, days=7)
            total_11_day = time.percent_start_records_created_within_x_days(**parameters, days=11)
            total_14_day = time.percent_start_records_created_within_x_days(**parameters, days=14)
            
            day_1_percentage = "{:.1%}".format(total_1_day)
            day_4_percentage = "{:.1%}".format(total_4_day)
            day_7_percentage = "{:.1%}".format(total_7_day)
            day_11_percentage = "{:.1%}".format(total_11_day)
            day_14_percentage = "{:.1%}".format(total_14_day)
            
            header_text = ["Record Creation - Project Start","%","Goal"]
            formatted_header_text = [Paragraph(cell, rf.tableSecondaryHeader) for cell in header_text]

            creation_data = [formatted_header_text]

            creation_data.append([Paragraph("Created within 1 day", rf.tableTextStyle), Paragraph(day_1_percentage, rf.textColor(day_1_percentage,0.80)),Paragraph(day_1_threshold, rf.tableTextStyleCenter) ])
            creation_data.append([Paragraph("Created within 4 days", rf.tableTextStyle),Paragraph(day_4_percentage, rf.textColor(day_4_percentage,0.85)),Paragraph(day_4_threshold, rf.tableTextStyleCenter)])
            creation_data.append([Paragraph("Created within 7 days", rf.tableTextStyle), Paragraph(day_7_percentage, rf.textColor(day_7_percentage,0.90)),Paragraph(day_7_threshold, rf.tableTextStyleCenter)])
            creation_data.append([Paragraph("Created within 11 days", rf.tableTextStyle),Paragraph(day_11_percentage, rf.textColor(day_11_percentage,0.95)),Paragraph(day_11_threshold, rf.tableTextStyleCenter)])
            creation_data.append([Paragraph("Created within 14 days", rf.tableTextStyle),Paragraph(day_14_percentage, rf.textColor(day_14_percentage,1.0)),Paragraph(day_14_threshold, rf.tableTextStyleCenter)])

            creation_table= Table(creation_data, colWidths=[2.2*inch,.7*inch,.7*inch], rowHeights=[inch/5]*len(creation_data),style=rf.demosTableStyle)
            
            ##time-exit
            total_1_day_exit = time.percent_exit_records_created_within_x_days(**parameters, days=1)
            total_4_day_exit = time.percent_exit_records_created_within_x_days(**parameters, days=4)
            total_7_day_exit = time.percent_exit_records_created_within_x_days(**parameters, days=7)
            total_11_day_exit = time.percent_exit_records_created_within_x_days(**parameters, days=11)
            total_14_day_exit = time.percent_exit_records_created_within_x_days(**parameters, days=14)
            
            day_1_percentage_exit = "{:.1%}".format(total_1_day_exit)
            day_4_percentage_exit = "{:.1%}".format(total_4_day_exit)
            day_7_percentage_exit = "{:.1%}".format(total_7_day_exit)
            day_11_percentage_exit = "{:.1%}".format(total_11_day_exit)
            day_14_percentage_exit = "{:.1%}".format(total_14_day_exit)

            header_text = ["Record Creation - Project Exit","%","Goal"]
            formatted_header_text = [Paragraph(cell, rf.tableSecondaryHeader) for cell in header_text]
            exit_data = [formatted_header_text]

            exit_data.append([Paragraph("Created within 1 day", rf.tableTextStyle), Paragraph(day_1_percentage_exit, rf.textColor(day_1_percentage_exit,0.80)),Paragraph(day_1_threshold, rf.tableTextStyleCenter)])
            exit_data.append([Paragraph("Created within 4 days", rf.tableTextStyle),Paragraph(day_4_percentage_exit, rf.textColor(day_4_percentage_exit, 0.85)),Paragraph(day_4_threshold, rf.tableTextStyleCenter)])
            exit_data.append([Paragraph("Created within 7 days", rf.tableTextStyle), Paragraph(day_7_percentage_exit, rf.textColor(day_7_percentage_exit,0.90)),Paragraph(day_7_threshold, rf.tableTextStyleCenter)])
            exit_data.append([Paragraph("Created within 11 days", rf.tableTextStyle), Paragraph(day_11_percentage_exit, rf.textColor(day_11_percentage_exit,0.95)),Paragraph(day_11_threshold, rf.tableTextStyleCenter)])
            exit_data.append([Paragraph("Created within 14 days", rf.tableTextStyle),Paragraph(day_14_percentage_exit, rf.textColor(day_14_percentage_exit,1.0)),Paragraph(day_14_threshold, rf.tableTextStyleCenter)])
            
            exit_table = Table(exit_data, colWidths=[2.2*inch,.7*inch,.7*inch], rowHeights=[inch/5]*len(exit_data),style=rf.demosTableStyle)
            row1demotable = Table([[creation_table,exit_table]],style=rf.demoPageTableStyle)
            
            header_text = ["Average Days of Record Creation - Entry"]
            formatted_header_text = [Paragraph(cell, rf.tableSecondaryHeader) for cell in header_text]
            average_entry_data = [formatted_header_text]
            average_entry_days="{:.1f}".format(time.record_creation_start_average(**parameters))
            if average_entry_days!='0.0':
                average_entry_data.append([Paragraph(f"{average_entry_days} days",rf.tableValuesStyleBig)])
            else:
                average_entry_data.append([Paragraph("No Clients Enrolled",rf.tableValuesStyleBig)])
            average_entry_table = Table(average_entry_data, colWidths=[3.5*inch], rowHeights=[inch/4]*len(average_entry_data),style=rf.demosTableStyle)
            
            header_text = ["Average Days of Record Creation - Exit"]
            formatted_header_text = [Paragraph(cell, rf.tableSecondaryHeader) for cell in header_text]
            average_exit_data = [formatted_header_text]
            average_exit_days="{:.1f}".format(time.record_creation_exit_average(**parameters))
            if average_exit_days!='0.0':
                average_exit_data.append([Paragraph(f"{average_exit_days} days",rf.tableValuesStyleBig)])
            else:
                average_exit_data.append([Paragraph("No Program Exits",rf.tableValuesStyleBig)])
            average_exit_table = Table(average_exit_data, colWidths=[3.5*inch], rowHeights=[inch/4]*len(average_exit_data),style=rf.demosTableStyle)
            row2demotable = Table([[average_entry_table,average_exit_table]],style=rf.demoPageTableStyle)
            
            page1_demographics = [[row1demotable],[row2demotable]
                                ]
            page1_demographics_table = Table(page1_demographics, style=rf.demoPageTableStyle)
            self.elements.append(page1_demographics_table)

            
    def walkerGrid(self, program_name=None, program_id=None):    
        parameters = {"start_date": self.start_date, "end_date": self.end_date} 
        parameters.update({"program_id": [program_id]})
        program_count="{:,}".format(dq.active_clients(**parameters)) 
        if program_count!='0':           
            self.elements.append(Paragraph(f"{program_name} Data Quality Participant Concern List", rf.pageHeaderStylesmall))
            #self.elements.append(Spacer(0, inch/4))
            
            # Get participant list
            pt = dq.pt_list(**parameters)

            # Define table headers
            header_text = ["Unique ID", "Case Manager","Personal Data Quality", "Universal Data Quality","Income & Housing Data Quality"]
            formatted_header_text = [Paragraph(cell, rf.tableSecondaryHeaderPurple) for cell in header_text]
            data = [formatted_header_text]

            # Create and append table rows for each participant
            for participant_id,issue_type in pt.items():
                row_data = [Paragraph(participant_id[0], rf.tableTextStyleCenter), Paragraph(participant_id[1], rf.tableTextStyleCenter),'', '', '']  # Initialize row data with empty strings
                # Check the type of issue associated with the participant
                if issue_type == 'personal_issue':
                    row_data[2] = Paragraph('✘', rf.tableTextStyleCenterRed) 
                    row_data[3] = Paragraph("✔", rf.tableTextStyleCenterGreen)
                    row_data[4] = Paragraph("✔", rf.tableTextStyleCenterGreen)
                elif issue_type == 'universal_issue':
                    row_data[2] = Paragraph("✔", rf.tableTextStyleCenterGreen)
                    row_data[3] = Paragraph('✘', rf.tableTextStyleCenterRed)
                    row_data[4] = Paragraph("✔", rf.tableTextStyleCenterGreen)# Mark 'Universal' column with 'X'
                elif issue_type == 'personal/universal':
                    row_data[2] = Paragraph('✘', rf.tableTextStyleCenterRed)
                    row_data[3] = Paragraph('✘', rf.tableTextStyleCenterRed)
                    row_data[4] = Paragraph("✔", rf.tableTextStyleCenterGreen)
                elif issue_type == 'personal/income':
                    row_data[2] = Paragraph('✘', rf.tableTextStyleCenterRed)
                    row_data[3] = Paragraph("✔", rf.tableTextStyleCenterGreen)
                    row_data[4] = Paragraph('✘', rf.tableTextStyleCenterRed)
                elif issue_type == 'universal/income':
                    row_data[2] = Paragraph("✔", rf.tableTextStyleCenterGreen)
                    row_data[3] = Paragraph('✘', rf.tableTextStyleCenterRed)
                    row_data[4] = Paragraph('✘', rf.tableTextStyleCenterRed)
                elif issue_type == 'income_issue':
                    row_data[2] = Paragraph("✔", rf.tableTextStyleCenterGreen)
                    row_data[3] = Paragraph("✔", rf.tableTextStyleCenterGreen)
                    row_data[4] = Paragraph('✘', rf.tableTextStyleCenterRed)
                elif issue_type == 'personal/universal/income':
                    row_data[2] = Paragraph('✘', rf.tableTextStyleCenterRed)
                    row_data[3] = Paragraph('✘', rf.tableTextStyleCenterRed)
                    row_data[4] = Paragraph('✘', rf.tableTextStyleCenterRed)
                # Append row data to table data
                data.append(row_data)

            # Create the table
            table = Table(data, colWidths=[1.3*inch,1.4*inch,1.3*inch,1.4*inch,1.8*inch], style=rf.ptTableStyle)

            # Append the table to self.elements
            self.elements.append(table)
            note_text = "**This list contains HMIS Unique IDs for participants with data concerns in either personal identifiable information, universal data elements, or income & housing information.**"
            self.elements.append(Paragraph(note_text, rf.noteStyle2))

            self.elements.append(PageBreak())

    def division1(self, region=None, department=None,program_id=None):

        print(f"   Starting Region/Department Data Quality for {region,department}")
        
        self.elements.append(Paragraph(f"{department} Data Quality", rf.pageHeaderStyle))
        self.elements.append(Spacer(0,inch/6))
        self.department_dataquality(department=department,program_id=program_id)
        self.elements.append(Spacer(0,inch/15))
        self.department_charts(department=department)
        self.elements.append(PageBreak())
        self.elements.append(Paragraph(f"Data Entry Timeliness", rf.pageHeaderStyle))
        self.elements.append(Spacer(0,inch/15))
        self.department_timeliness(department=department,program_id=program_id)
        self.elements.append(PageBreak())
        
        print(f"   Ending Region/Department Data Quality for {region,department}")

    def department_division1(self, department=None):

        print(f"   Starting Region/Department Data Quality for {department}")
        
        self.elements.append(Paragraph(f"{department} Data Quality", rf.pageHeaderStyle))
        self.elements.append(Spacer(0,inch/6))
        self.executive_department_dataquality(department=department)
        self.elements.append(Paragraph(f"Data Entry Timeliness", rf.pageHeaderStyle))
        self.elements.append(Spacer(0,inch/10))
        self.program_timeliness(department=department)
        self.elements.append(PageBreak())
        
        print(f"   Ending Region/Department Data Quality for {department}") 
    
    def executive_division1(self):

        print(f"   Starting Executive Overview")
        
        self.elements.append(Paragraph(f"Agency Data Quality", rf.pageHeaderStyle))
        self.elements.append(Spacer(0,inch/6))
        self.agency_dataquality()
        self.elements.append(Paragraph(f"Data Entry Timeliness", rf.pageHeaderStyle2))
        self.elements.append(Spacer(0,inch/10))
        self.program_timeliness()
        self.elements.append(PageBreak())
        
        print(f"  Ending Executive Overview")

    def division2(self,program=None, mergedid=None):
        
        
        print(f"Starting DQ Participant List for {program}")
        
        self.walkerGrid(program,mergedid)
        
        print(f"Ending DQ Participant List for {program}")
        
    def division3(self,department=None,program_type=None,program_id=None):
        print(f"   Starting Region/Department Data Quality for {program_id}")
        
        self.program_dataquality(department=department,program_type=program_type,program_id=program_id)
        self.elements.append(Paragraph(f"Data Entry Timeliness", rf.pageHeaderStyle2))
        self.elements.append(Spacer(0,inch/10))
        self.program_timeliness(program_id=program_id)
        self.elements.append(PageBreak())


        print(f"   Ending Region/Department Data Quality for {program_id}")    
    def programPage(self, department,program_type, program_name, program_dict):
        print(department)
        if department=='Sanata Barbara':
            print('YES')
        elif department==['Santa Barbara']:
            print('yes')
        self.elements.append(Paragraph(program_name, rf.programHeaderStyle))
        self.elements.append(Spacer(0,inch/12))
        if department!=['Santa Barbara']:
            for MergedID, MergedIDData in program_dict.items():
                # Program information header
                DataSystemProgramName = MergedIDData['DataSystemProgramName']
                PrimaryDataSystem = MergedIDData['PrimaryDataSystem']
                DataSystemID = MergedIDData['DataSystemID']
                                    
                program_info = ["Data system name",DataSystemProgramName, "Primary data system",PrimaryDataSystem, "Data system ID", DataSystemID]
                program_info_table = Table([program_info], style=rf.progInfoTableStyle, colWidths=rf.colummWidths(PrimaryDataSystem), rowHeights=[inch/6])

                self.elements.append(program_info_table)
                if PrimaryDataSystem != 'HMIS':
                        self.elements.append(Paragraph("Note: This is a non-HMIS program, numbers should be interpreted with caution.", rf.noteStyle))
                        self.elements.append(Spacer(0,inch/8))
                        print("Loaded header:", MergedID)
                        self.division3(department,program_type,MergedID)
                else:  
                        self.elements.append(Spacer(0,inch/8))
                        print("Loaded header:", MergedID)
                        self.division3(department,program_type,MergedID)
                        #self.division2(DataSystemProgramName,MergedID)
        else:
            for MergedID, MergedIDData in program_dict.items():
                # Program information header
                DataSystemProgramName = MergedIDData['DataSystemProgramName']
                PrimaryDataSystem = MergedIDData['PrimaryDataSystem']
                DataSystemID = MergedIDData['DataSystemID']
                                    
                program_info = ["Data system name",DataSystemProgramName, "Primary data system",PrimaryDataSystem, "Data system ID", DataSystemID]
                program_info_table = Table([program_info], style=rf.progInfoTableStyle, colWidths=rf.colummWidths(PrimaryDataSystem), rowHeights=[inch/6])

                self.elements.append(program_info_table)
                if PrimaryDataSystem != 'HMIS':
                        self.elements.append(Paragraph("Note: This is a non-HMIS program, numbers should be interpreted with caution.", rf.noteStyle))
                        self.elements.append(Spacer(0,inch/8))
                        print("Loaded header:", MergedID)
                        self.division3(department,program_type,MergedID)
                else:  
                        self.elements.append(Spacer(0,inch/8))
                        print("Loaded header:", MergedID)
                        self.division3(department,program_type,MergedID)

            
    
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
                            if "average" in indicator_name.lower() or "mean" in indicator_name.lower():
                                functionResult = Paragraph("-", rf.tableValuesStyle)
                            else:
                                functionResult = Paragraph("Coming<br/>Soon", rf.tableValuesStyle)
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

def get_reports():
    report_type = None
    report_cadence = None
    master_dict=m.all_programs_dict()

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
                
                elif not dept_input:
                    print("Creating All Department Reports")
                    master_dict = m.all_programs_dict()
                    for Region, DeptDict in master_dict.items():
                        for Dept, ProgTypeDict in DeptDict.items():
                            QuarterlyReports("Monthly", Dept, fy_name, cadence_name, start_date, end_date, fy_start_date)
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
                    QuarterlyReports("Quarterly", full_dept, fy_name, cadence_name, start_date, end_date, fy_start_date)
                
                elif not dept_input:
                    print("Creating All Department Reports")
                    master_dict = m.all_programs_dict()
                    for Region, DeptDict in master_dict.items():
                        for Dept, ProgTypeDict in DeptDict.items():
                            QuarterlyReports("Quarterly", Dept, fy_name, cadence_name, start_date, end_date, fy_start_date)
                    QuarterlyReports("Quarterly", "Los Angeles County", fy_name, cadence_name, start_date, end_date, fy_start_date)

                    break 
                else:
                    print("\nInvalid input!")

        else:
            print("\nInvalid input!\n")


if __name__ == '__main__':
    
    get_reports()