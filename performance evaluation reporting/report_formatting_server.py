import os

from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.enums import TA_LEFT, TA_RIGHT, TA_CENTER, TA_JUSTIFY
from reportlab.lib.colors import Color, gray, lightgrey, black, white,linen
from reportlab.lib.pagesizes import inch
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics
from reportlab.platypus import TableStyle


import FY24_Report_server as report


# Register Fonts
pdfmetrics.registerFont(TTFont('Montserrat', os.path.join(report.font_directory, 'Montserrat-Regular.ttf')))
pdfmetrics.registerFont(TTFont('MontserratSemiBold', os.path.join(report.font_directory, 'Montserrat-SemiBold.ttf')))
pdfmetrics.registerFont(TTFont('OpenSans', os.path.join(report.font_directory, 'OpenSans-Regular.ttf')))
pdfmetrics.registerFont(TTFont('OpenSansBold', os.path.join(report.font_directory, 'OpenSans-Bold.ttf')))
pdfmetrics.registerFont(TTFont('OpenSansItalic', os.path.join(report.font_directory, 'OpenSans-Italic.ttf')))


# Color definitions
PATHBlue = Color((0.0/255), (174.0/255), (239.0/255), 1)
PATHLightBlue = Color((178/255), (231/255), (250.0/255), 1)
PATHPurple = Color((138.0/255), (40.0/255), (143.0/255), 1)
PATHLightPurple = Color((220/255), (190/255), (221/255), 1)
PATHRed = Color((237/255), (28/255), (36/255), 1)
PATHLightRed = Color((250/255), (186/255), (189/255), 1)
PATHGreen = Color((34.0/255), (178/255), (76/255), 1)
PATHLightGreen = Color((188.0/255), (232.0/255), (201.0/255), 1)

# Paragraph Styles and Other Text Formatting
titlePageStyle = ParagraphStyle('Title Page',fontName="Montserrat",fontSize=35, leading=35, alignment=TA_LEFT, justifyLastLine=1)
titlesubPageStyle = ParagraphStyle('Title Page',fontName="MontserratSemiBold",fontSize=80, leading=35, alignment=TA_LEFT)
titlePageSubHeaderStyle = ParagraphStyle('Title Page Sub',fontName="OpenSans",fontSize=12, leading=12, alignment=TA_LEFT)
pageHeaderStyle = ParagraphStyle('Page Header', fontName="MontserratSemiBold", fontSize=24, leading=20, alignment=TA_CENTER, textColor=PATHBlue)
pageSubHeaderStyle = ParagraphStyle('Page Sub-Header', fontName="MontserratSemiBold", fontSize=20, leading=20, alignment=TA_CENTER, textColor=PATHPurple)
sectionHeaderStyle = ParagraphStyle('Section Header', fontName="MontserratSemiBold", fontSize=16, leading=24, alignment=TA_LEFT, borderWidth=3)
subSectionHeaderStyle = ParagraphStyle('Sub-Section Section Header', fontName="Montserrat", fontSize=14, alignment=TA_CENTER, borderWidth=3)
programHeaderStyle = ParagraphStyle('Program Header', fontName="MontserratSemiBold", fontSize=12, alignment=TA_LEFT, borderWidth=3, leftIndent=8, textColor=PATHGreen)
tableHeaderStyle = ParagraphStyle('Table Header', fontName="Montserrat", fontSize=10, leading=15, alignment=TA_LEFT, borderWidth=0, textColor=PATHPurple)
tableSubHeader = ParagraphStyle('Table Text', fontName="MontserratSemiBold", fontSize=8, leading=12, alignment=TA_LEFT, leftIndent=8)
tableSecondaryHeader = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=8, leading=12, alignment=TA_CENTER)
tableTextStyle = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=8, leading=12, alignment=TA_RIGHT, borderWidth=4)
tableValuesStyle = ParagraphStyle('Table Values', fontName="OpenSansBold", fontSize=8, leading=12, alignment=TA_CENTER, borderWidth=4)
ComingSoonStyle = ParagraphStyle('Table Values', fontName="OpenSansBold", fontSize=8, leading=10, alignment=TA_CENTER, borderWidth=4)
tableTargetStyle = ParagraphStyle('Table Values', fontName="OpenSansItalic", fontSize=8, alignment=TA_CENTER, borderWidth=0)
tableCadenceStyle = ParagraphStyle('Table Values', fontName="OpenSans", fontSize=8, alignment=TA_CENTER, borderWidth=0)
tableFooterStyle = ParagraphStyle('Table Footer', fontName="OpenSansItalic", fontSize=6, leading=8, alignment=TA_JUSTIFY, borderWidth=0)
tableValuesExceedStyle = ParagraphStyle('Table Values', fontName="OpenSansBold", fontSize=8, alignment=TA_CENTER, borderWidth=0, textColor=PATHGreen)
tableValuesMissStyle = ParagraphStyle('Table Values', fontName="OpenSansBold", fontSize=8, alignment=TA_CENTER, borderWidth=0, textColor=PATHRed)
TOCSectionStyle = ParagraphStyle('TOCSectionStyle', fontName="Montserrat", fontSize=11, alignment=TA_LEFT)
TOCSubSectionStyle = ParagraphStyle('TOCSubSectionStyle', fontName="Montserrat", fontSize=9, alignment=TA_LEFT, leftIndent=8)
TOCHeaderStyle = ParagraphStyle('TOCSectionStyle', fontName="Montserrat", fontSize=24, alignment=TA_CENTER)
programsummarytitlePageStyle = ParagraphStyle('programsummarytitlePageStyle',fontName="Montserrat",fontSize=40, leading=35, alignment=TA_LEFT, justifyLastLine=1)

# KPI Tables
KPItableHeaderStyle = ParagraphStyle('Table Header', fontName="Montserrat", fontSize=10, leading=15, alignment=TA_LEFT, borderWidth=0, textColor=PATHPurple)
KPItableSecondaryHeader = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=8, leading=12, alignment=TA_CENTER)
KPItableIndcatorStyle = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=8, leading=12, alignment=TA_RIGHT)
KPItableValuesStyle = ParagraphStyle('Table Values', fontName="OpenSansBold", fontSize=8, alignment=TA_CENTER, borderWidth=0)
KPItableTargetStyle = ParagraphStyle('Table Values', fontName="OpenSansItalic", fontSize=8, alignment=TA_CENTER, borderWidth=0)
KPItableCadenceStyle = ParagraphStyle('Table Values', fontName="OpenSans", fontSize=8, alignment=TA_CENTER, borderWidth=0)
KPItableFooterStyle = ParagraphStyle('Table Footer', fontName="OpenSansItalic", fontSize=6, leading=8, alignment=TA_JUSTIFY, borderWidth=0)


# Agency Indicator by Dept Table
IndicatorByDeptIndicatorNameStyle = ParagraphStyle('Table Header', fontName="MontserratSemiBold", fontSize=9, leading=9, alignment=TA_LEFT, borderWidth=0)


# Diability Table
DisabilityTextStyle = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=8, leading=12, alignment=TA_LEFT, borderWidth=4)
DisabilitySevereTextStyle = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=8, leading=12, alignment=TA_LEFT, borderWidth=4, leftIndent=8)
DisabilitySevereAlcTextStyle = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=8, leading=12, alignment=TA_LEFT, borderWidth=4, leftIndent=16)

# Vet + CH Table
VetCHTextStyle = ParagraphStyle('Table Text', fontName="MontserratSemiBold", fontSize=28, leading=12, alignment=TA_CENTER, borderWidth=4)

# Agency Indicator Styles
IndicTextStyle = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=8, leading=8, alignment=TA_CENTER, borderWidth=4)
IndicNameTextStyle = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=8, leading=8, alignment=TA_LEFT, borderWidth=4, leftIndent=4)
IndicProgTypeStyle = ParagraphStyle('Table Header', fontName="Montserrat", fontSize=10, leading=15, alignment=TA_LEFT, borderWidth=0, textColor=PATHPurple)
IndicSecondaryHeader = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=9, leading=12, alignment=TA_CENTER)
IndicResult = ParagraphStyle('Table Text', fontName="OpenSansBold", fontSize=8, leading=12, alignment=TA_CENTER)

# Glossary Styles
GlossaryNameStyle = ParagraphStyle('Table Text', fontName="MontserratSemiBold", fontSize=9, leading=12, alignment=TA_RIGHT, borderWidth=4)
GlossaryDefinitionStyle = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=9, leading=12, alignment=TA_JUSTIFY, borderWidth=4)

# Reformat Agency Indicator by Dept Function Names - allows for wordwrap
styles = getSampleStyleSheet()
styles['Normal'].fontName = 'OpenSans'
styles['Normal'].fontSize = 9
functionNameStyle = styles['Normal']
functionNameStyle.wordWrap = 'CJK'


# Table Styles
#not used
progTypeIndicatorTableStyle = TableStyle([
    ('BACKGROUND', (0, 1), (-1, -1), linen),  
    ('LINEBELOW', (0, 1), (-1, 1), .2, gray),
    ('LINEBELOW', (0, 2), (-1, 2), .2, gray),
    ('LINEBELOW', (0, 3), (-1, 3), .2, gray),
    ('LINEBELOW', (0, 4), (-1, 4), .2, gray),
    ('LINEBELOW', (0, 5), (-1, 5), .2, gray),
    ('LINEBELOW', (0, 6), (-1, 6), .2, gray),
    ('LINEBELOW', (0, 7), (-1, 7), .2, gray),
    
    ('SPAN', (0, 0), (-1, 0)),
    ('LINEBELOW', (0, 0), (-1, 0), .8, black),
    ('VALIGN', (0, 0), (-1, -1), 'CENTER'),  
    ('ALIGN', (1, 2), (-1, -1), 'CENTER'),
    ('LINEBELOW', (0, -1), (-1, -1), .8, black),
    ('LEFTPADDING', (0, 1), (-1, -1), 0),
    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 0), 

])

AgencyIndicatorTableStyle = TableStyle([
    ('LINEABOVE', (0, 0), (-1, 0), .5, black),
    ('LINEBELOW', (0, -1), (-1, -1), .5, black),
    
    ('LINEBELOW', (0, 0), (-1, 0), .2, gray),
    ('LINEBELOW', (0, 1), (-1, 1), .2, gray),
    ('LINEBELOW', (0, 2), (-1, 2), .2, gray),
    ('LINEBELOW', (0, 3), (-1, 3), .2, gray),
    ('LINEBELOW', (0, 4), (-1, 4), .2, gray),
    ('LINEBELOW', (0, 5), (-1, 5), .2, gray),
    ('LINEBELOW', (0, 6), (-1, 6), .2, gray),
    ('LINEBELOW', (0, 7), (-1, 7), .2, gray),

    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),  
    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
    ('LEFTPADDING', (0, 0), (-1, -1), 0),
    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 0), 
])

AgencyIndicatorTableStructureStyle = TableStyle([
    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),  
    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
    ('LEFTPADDING', (0, 0), (-1, -1), 0),
    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 0), 
    ('LINEABOVE', (0, 2), (-1, 2), 1, black),
    ('LINEBELOW', (0, -1), (-1, -1), 1, black),

])

AgencyIndicatorHeaderStyle = TableStyle([
    ('BACKGROUND', (0,0), (-1,0), PATHLightBlue),
    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),  
    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
    ('LEFTPADDING', (0, 0), (-1, -1), 0),
    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 0), 

])

agencyIndicatorsByDeptRegionTableStyle = TableStyle([
    ('BACKGROUND', (1, 0), (-1, 0), PATHLightPurple),  
    ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ('FONTNAME', (0, 0), (-1, 0), 'MontserratSemiBold'),
    ('FONTSIZE', (1, 0), (-1, -1), 9),
    ('BACKGROUND', (0, 1), (-1, -1), (1,1,1)),
    ('GRID', (0, 1), (-1, -1), .3, gray),
    ('GRID', (1, 0), (-1, -1), .3, gray),
    ('LINEABOVE', (1, 0), (-2, 0), 1, (0, 0, 0)),
    ('LINEBELOW', (0, -1), (-2, -1), 1, (0, 0, 0)),
    ('LINEBEFORE', (1, 0), (1, 0), 1, (0, 0, 0)),
    ('LINEBEFORE', (0, 1), (0, -1), 1, (0, 0, 0)),
    ('LINEABOVE', (0, 1), (0, 1), 1, (0, 0, 0)),
    ('BACKGROUND', (0, 2), (-1, 2), lightgrey),
    ('BOX', (-1, 0), (-1, -1), 2, PATHGreen), 
    ('LEFTPADDING', (0, 0), (0, -1), 4),
    ('RIGHTPADDING', (0, 0), (0, -1), 3),
])

agencyIndicatorsByDeptDeptTableStyle = TableStyle([
    ('BACKGROUND', (0, 0), (-1, 0), PATHLightGreen),  
    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ('FONTNAME', (0, 0), (-1, 0), 'MontserratSemiBold'),        
    ('FONTSIZE', (0, 0), (-1, -1), 9),
    ('FONTNAME', (0, 1), (-1, -1), 'OpenSans'),
    ('BACKGROUND', (0, 1), (-1, -1), (1,1,1)),
    ('GRID', (0, 1), (-1, -1), .5, gray),
    ('GRID', (1, 0), (-1, -1), .3, gray),
    ('BOX', (0, 0), (-1, -1), 2, PATHGreen),
    ('BACKGROUND', (0, 2), (-1, 2), lightgrey),  
])

agencyIndicatorsAllAgencyTableStyle = TableStyle([
    ('BACKGROUND', (0, 0), (-1, 0), PATHLightBlue), 
    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),    
    ('FONTSIZE', (0, 0), (-1, -1), 9),
    ('FONTNAME', (0, 0), (-1, -1), 'MontserratSemiBold'),
    ('BACKGROUND', (0, 1), (-1, -1), (1,1,1)),
    ('GRID', (0, 1), (-1, -1), .5, gray),
    ('GRID', (1, 0), (-1, -1), .3, gray),
    ('BOX', (0, 0), (-1, -1), 2, PATHBlue),
    ('BACKGROUND', (0, 2), (-1, 2), lightgrey),  
])


agencyIndicatorsByDeptContainerTableStyle = TableStyle([
    ('LINEBELOW', (1, 0), (-3, 0), 1, PATHGreen, None, (1,2)),
    ('LINEABOVE', (1, 0), (-3, 0), 1, PATHGreen, None, (1,2)),
    ('LEFTPADDING', (0, 0), (-1, -1), 0),
    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 0)
])


demosTableStyle = TableStyle([
    ('BACKGROUND', (0, 0), (-1, -1), linen),     
    ('LINEBELOW', (0, 0), (-1, 0), .8, black),
    ('LINEBELOW', (0, -1), (-1, -1), .8, black),
    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ('LINEBELOW', (0, 1), (-1, 1), .2, gray),
    ('LINEBELOW', (0, 2), (-1, 2), .2, gray),
    ('LINEBELOW', (0, 3), (-1, 3), .2, gray),
    ('LINEBELOW', (0, 4), (-1, 4), .2, gray),
    ('LINEBELOW', (0, 5), (-1, 5), .2, gray),
    ('LINEBELOW', (0, 6), (-1, 6), .2, gray),
    ('LINEBELOW', (0, 7), (-1, 7), .2, gray),
    ('LINEBELOW', (0, 8), (-1, 8), .2, gray),
    ('LINEBELOW', (0, 9), (-1, 9), .2, gray),
    ('LINEBELOW', (0, 10), (-1, 10), .2, gray),
    ('LINEBELOW', (0, 11), (-1, 11), .2, gray),
    ('LINEBELOW', (0, 12), (-1, 12), .2, gray),
    ('LINEBELOW', (0, 13), (-1, 13), .2, gray),
    ('LINEBELOW', (0, 14), (-1, 14), .2, gray),

])

DisabilityTableStyle = TableStyle([
    ('BACKGROUND', (0, 0), (-1, -1), linen),     
    ('LINEBELOW', (0, 0), (-1, 0), .8, black),
    ('LINEBELOW', (0, -2), (-1, -2), .8, black),
    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ('LINEBELOW', (0, 1), (-1, 1), .2, gray),
    ('LINEBELOW', (0, 2), (-1, 2), .2, gray),
    ('LINEBELOW', (0, 3), (-1, 3), .2, gray),
    ('LINEBELOW', (0, 4), (-1, 4), .2, gray),
    ('LINEBELOW', (0, 5), (-1, 5), .2, gray),
    ('LINEBELOW', (0, 6), (-1, 6), .2, gray),
    ('LINEBELOW', (0, 7), (-1, 7), .2, gray),
    ('LINEBELOW', (0, 8), (-1, 8), .2, gray),
    ('LINEBELOW', (0, 9), (-1, 9), .2, gray),
    ('LINEBELOW', (0, 10), (-1, 10), .2, gray),
    ('LINEBELOW', (0, 11), (-1, 11), .2, gray),
    ('LINEBELOW', (0, 12), (-1, 12), .2, gray),
    ('LINEBELOW', (0, 13), (-1, 13), .2, gray),
    ('LINEBELOW', (0, 14), (-1, 14), .2, gray),
    ('RIGHTPADDING', (0, 11), (0, -2), 20),
    ('SPAN', (0, -1), (-1, -1))


])


dismergeTableStyle = TableStyle([
    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
    ('LEFTPADDING', (0, 0), (-1, -1), 0),
    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
    ])

VetStatusTableStyle = TableStyle([
    ('BACKGROUND', (0, 0), (-1, -1), PATHLightGreen),
    ('VALIGN', (0, 0), (-1, -1), 'TOP'),

    ])

CHStatusTableStyle = TableStyle([
    ('BACKGROUND', (0, 0), (-1, 0), PATHLightBlue),
     ('VALIGN', (0, 0), (-1, -1), 'TOP'),

    ])

demoPageTableStyle = TableStyle([
    
    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
    ('LEFTPADDING', (0, 0), (-1, -1), 0),
    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
    ])

demoPage2TableStyle = TableStyle([
    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
    ('SPAN', (0, 0), (-1, 0)),
    ('SPAN', (0, 1), (-1, 1)),

    ('LEFTPADDING', (0, 0), (-1, -1), 0),
    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
    ])

#not used
regionSummaryAgencyIndicatorTableStyle = TableStyle([
    ('GRID', (0, 0), (-1, -1), .3, gray),
    ('BOX', (0, 0), (-1, -1), 2, (0, 0, 0)),
    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
    ('FONTNAME', (0, 0), (0, -1), 'OpenSans'),
    ('FONTNAME', (-1, 0), (-1, -1), 'MontserratSemiBold')
])

#not used
regionSummaryAlignmentTableStyle = TableStyle([
    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
    ('SPAN', (0, -1), (-1, -1))
])

progInfoTableStyle = TableStyle([
    ('LINEABOVE', (0, 0), (-1, 0), .5, black),
    ('FONT', (0, 0), (0, 0), 'OpenSans', 7),
    ('FONT', (1, 0), (1, 0), 'OpenSansBold', 6),
    ('FONT', (2, 0), (2, 0), 'OpenSans', 7),
    ('FONT', (3, 0), (3, 0), 'OpenSansBold', 6),
    ('FONT', (4, 0), (4, 0), 'OpenSans', 7),
    ('FONT', (5, 0), (5, 0), 'OpenSansBold', 6),
    ('VALIGN', (0, 0), (-1, -1), "MIDDLE"),
    ('LINEBELOW', (0, -1), (-1, -1), .5, black),
    ('COLBACKGROUNDS', (0, 0), (-1, -1), [white, lightgrey]),
    ('ALIGN', (0, 0), (0, 0), "CENTER"),
    ('ALIGN', (1, 0), (1, 0), "LEFT"),
    ('ALIGN', (2, 0), (2, 0), "CENTER"),
    ('ALIGN', (3, 0), (3, 0), "CENTER"),
    ('ALIGN', (4, 0), (4, 0), "CENTER"),
    ('ALIGN', (5, 0), (5, 0), "CENTER"),

    
])

#not used
grantInfoTableStyle = TableStyle([
    ('LINEABOVE', (0, 0), (-1, 0), .5, black),
    ('FONT', (0, 0), (0, -1), 'OpenSans', 7),
    ('FONT', (1, 0), (1, -1), 'OpenSansBold', 6),
    ('ALIGN', (0, 0), (-1, -1), "LEFT"),
    ('VALIGN', (0, 0), (-1, -1), "MIDDLE"),
    ('LINEBELOW', (0, -1), (-1, -1), .5, black),
])

#not used
progAlignmentTableStyle = TableStyle([
    ('LEFTPADDING', (0, 0), (-1, -1), 0),
    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
    ('ALIGN', (0, 0), (0, 0), "LEFT"),
    ('ALIGN', (1, 0), (1, 0), "RIGHT"),
    ('VALIGN', (0, 0), (-1, -1), "TOP"),
])

programAgencyIndicatorsTableStyle = TableStyle([
    ('SPAN', (0, 0), (-1, 0)),
    ('ALIGN', (0, 0), (1, 0), 'CENTER'),
    ('FONT', (0, 0), (-1, 0), 'MontserratSemiBold', 9),
    ('BACKGROUND', (0, 0), (-1, 0), PATHLightGreen),
    ('LINEABOVE', (0, 1), (-1, 1), .5, black),
    ('FONT', (0, 1), (0, -1), 'OpenSans', 7),
    ('FONT', (1, 0), (1, -1), 'OpenSansBold', 7),
    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [white, lightgrey]),

])

ProgramLevelAgencyIndicatorsAlignmentTableStyle = TableStyle([
    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
    ('LEFTPADDING', (0, 0), (-1, -1), 0),
    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 10),

])

PersonsServedTableStyle = TableStyle([
    ('SPAN', (0, 0), (-1, 0)),
    ('SPAN', (0, -1), (-1, -1)),
    ('SPAN', (0, 4), (-1, 4)),
    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),  

    ('LEFTPADDING', (0, 0), (-1, -1), 2),
    ('RIGHTPADDING', (0, 0), (-1, -1), 2),
    ('TOPPADDING', (0, -2), (-1, -2), 2),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 2),

    ('LINEBELOW', (0, 0), (-1, 0), .8, black),
    ('LINEBELOW', (0, 1), (-1, 1), .3, black),  
    ('LINEBELOW', (0, 3), (-1, 3), .3, black),  
    ('LINEBELOW', (0, 4), (-1, 4), .3, black),  
    ('LINEBELOW', (0, -2), (-1, -2), .8, black)


])

PHPTableStyle = TableStyle([
    ('SPAN', (0, 0), (-1, 0)),
    ('SPAN', (0, -1), (-1, -1)),
    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),  


    ('LEFTPADDING', (0, 0), (-1, -1), 2),
    ('RIGHTPADDING', (0, 0), (-1, -1), 2),
    ('TOPPADDING', (0, -2), (-1, -2), 2),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 2),


    ('LINEBELOW', (0, 0), (-1, 0), .8, black),
    ('LINEBELOW', (0, 1), (-1, 1), .3, black),  
    ('LINEBELOW', (0, 3), (-1, 3), .3, black),  
    ('LINEBELOW', (0, -2), (-1, -2), .8, black)

])

Row1SpacerStyle = TableStyle([
    ('LINEBELOW', (0, 0), (-1, 0), .8, black),
    ('LINEBELOW', (0, -2), (-1, -2), .8, black)

])

IncomeIncreaseTableStyle = TableStyle([
    ('BACKGROUND', (0, 0), (-1, -1), lightgrey),
    
    ('SPAN', (0, 0), (-1, 0)),
    ('SPAN', (0, -1), (-1, -1)),
    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),  


    ('LEFTPADDING', (0, 0), (-1, -1), 2),
    ('RIGHTPADDING', (0, 0), (-1, -1), 2),
    ('TOPPADDING', (0, 0), (-1, -2), 2),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 2),


    ('LINEBELOW', (0, 0), (-1, 0), .8, black),
    ('LINEBELOW', (0, 1), (-1, 1), .3, black),  
    ('LINEBELOW', (0, 3), (-1, 3), .3, black),  
    ('LINEBELOW', (0, -2), (-1, -2), .8, black)

    
])

ServiceDQTableStyle = TableStyle([
    ('SPAN', (0, 0), (-1, 0)),  
    ('FONT', (0, 0), (-1, 0), 'Montserrat', 10),
    ('ALIGN', (0, 0), (-1, 0), 'LEFT'),
    ('LINEBELOW', (0, 0), (-1, 0), .8, black),

    ('FONT', (0, 1), (0, -2), 'OpenSans', 8),
    ('ALIGN', (0, 1), (0, -2), 'RIGHT'),    
    ('LINEBELOW', (0, 2), (-1, 2), .3, black),  

    ('ALIGN', (1, 1), (-1, -2), 'CENTER'),      
    ('FONT', (1, 1), (-1, -2), 'OpenSansBold', 8),
    ('LINEBELOW', (0, -2), (-1, -2), .8, black),  

    ('SPAN', (0, -1), (-1, -1)),  
    ('FONT', (0, -1), (-1, -1), 'OpenSansItalic', 7),
    ('ALIGN', (0, -1), (-1, -1), 'LEFT'),
    ('VALIGN', (0, 0), (-1, -2), 'MIDDLE'),
    
    ('LEFTPADDING', (0, 1), (-1, -2), 6),
    ('RIGHTPADDING', (0, 0), (-1, -1), 6),
    ('TOPPADDING', (0, 0), (-1, -1), 6),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
    ('BACKGROUND', (0, 0), (-1, -1), lightgrey),
    
])


utilTableStyle = TableStyle([
    ('SPAN', (0, 0), (-1, 0)),
    ('ALIGN', (0, 0), (0, 0), 'CENTER'),
    ('FONT', (0, 0), (-1, 0), 'MontserratSemiBold', 9),
    ('BACKGROUND', (0, 0), (-1, 0), PATHLightGreen),
    ('FONT', (0, -1), (0, -1), 'OpenSansBold', 8),
    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),

])

losTableStyle = TableStyle([
    ('SPAN', (0, 0), (-1, 0)),
    ('ALIGN', (0, 0), (0, 0), 'CENTER'),
    ('FONT', (0, 0), (-1, 0), 'MontserratSemiBold', 9),
    ('BACKGROUND', (0, 0), (-1, 0), PATHLightGreen),
    ('FONT', (0, -1), (0, -1), 'OpenSansBold', 8),
    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),

])

docTableStyle = TableStyle([
    ('SPAN', (0, 0), (-1, 0)),
    ('ALIGN', (0, 0), (0, 0), 'CENTER'),
    ('FONT', (0, 0), (-1, 0), 'MontserratSemiBold', 9),
    ('BACKGROUND', (0, 0), (-1, 0), PATHLightGreen),
    ('FONT', (0, -1), (0, -1), 'OpenSansBold', 8),
    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),

])

InterimIndicatorsTableStyle = TableStyle([
    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ('ALIGN', (0, 0), (-1, -1), 'Center'),
    ('LEFTPADDING', (0, 0), (-1, -1), 0),
    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
    ('GRID', (0, 0), (-1, -1), .3, gray),

])

InterimIndicatorsTableRow1Style = TableStyle([
    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ('ALIGN', (0, 0), (-1, -1), 'Center'),
    ('LEFTPADDING', (0, 0), (-1, -1), 0),
    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
    ('GRID', (0, 0), (-1, -1), .3, gray),

])

ProgramLevelProgTypeIndicatorsTableStyle = TableStyle([
    ('SPAN', (0, 0), (-1, 0)),  
    ('LINEBELOW', (0, 0), (-1, 0), .8, black),

    ('VALIGN', (0, 0), (-1, -1), 'CENTER'),      
    ('LINEBELOW', (0, -1), (-1, -1), .8, black),  

    
    ('LEFTPADDING', (0, 1), (-1, -1), 0),
    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 0),

    ])

allCatTableStyle = TableStyle([
    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
    ('ALIGN', (0, 0), (0, -1), 'LEFT'),
    ('LEFTPADDING', (0, 0), (-1, -1), 0),
    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 15),


    ])

ProgramLevelContractIndicatorsTableStyle = TableStyle([
    ('VALIGN', (-3, 1), (-1, -1), 'CENTER'),  
    ('ALIGN', (-3, 0), (-1, 0), 'RIGHT'),  
    ('SPAN', (-3, 0), (-1, 0)),  






    ('LINEBELOW', (0, 0), (-1, 0), .8, black),
    ('LINEBELOW', (0, -1), (-1, -1), .8, black),
    
    ('LEFTPADDING', (0, 1), (-1, -1), 0),

    ('RIGHTPADDING', (1, 0), (-1, -1), 0),
    ('TOPPADDING', (1, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (1, 0), (-1, -1), 0), 






    ('LINEBELOW', (0, 1), (-1, 1), .2, gray),
    ('LINEBELOW', (0, 2), (-1, 2), .2, gray),
    ('LINEBELOW', (0, 3), (-1, 3), .2, gray),
    ('LINEBELOW', (0, 4), (-1, 4), .2, gray),
    ('LINEBELOW', (0, 5), (-1, 5), .2, gray),
    ('LINEBELOW', (0, 6), (-1, 6), .2, gray),
    ('LINEBELOW', (0, 7), (-1, 7), .2, gray),
    ('LINEBELOW', (0, 8), (-1, 8), .2, gray),
    ('LINEBELOW', (0, 9), (-1, 9), .2, gray),
    ('LINEBELOW', (0, 10), (-1, 10), .2, gray),
    ('LINEBELOW', (0, 11), (-1, 11), .2, gray),
    ('LINEBELOW', (0, 12), (-1, 12), .2, gray),
    ('LINEBELOW', (0, 13), (-1, 13), .2, gray),
    ('LINEBELOW', (0, 14), (-1, 14), .2, gray),

])


row2DemotableStyle = TableStyle([
    ('GRID', (0, 0), (-1, -1), .3, gray),
    
    ('LEFTPADDING', (0, 0), (-1, -1), 0),

    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 0), 
])


SingleIndicatorTableStyle = TableStyle([
    ('SPAN', (0, 0), (-1, 0)),  
    ('LINEBELOW', (0, 0), (-1, 0), .8, black),
    ('VALIGN', (0, 1), (-1, -2), 'MIDDLE'),      
    ('LINEBELOW', (0, -2), (-1, -2), .8, black),  
    ('SPAN', (0, -1), (-1, -1)),  
    ('LINEBELOW', (0, 1), (-1, 1), .2, gray),
    ('VALIGN', (0, -1), (-1, -1), 'TOP'),      
    ('LEFTPADDING', (0, 1), (-1, -2), 0),
    ('RIGHTPADDING', (0, 0), (-2, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 0)
])

TwoOrMoreIndicatorTableStyle = TableStyle([
    ('SPAN', (0, 0), (1, 0)),  
    ('LINEBELOW', (0, 0), (-1, 0), .8, black),
    ('VALIGN', (0, 1), (-1, -2), 'MIDDLE'),      
    ('LINEBELOW', (0, -2), (-1, -2), .8, black),  
    ('SPAN', (0, -1), (-1, -1)),  
    ('LINEBELOW', (0, 1), (-1, 1), .2, gray),

    ('ROWBACKGROUNDS', (0, 2), (-1, -2), [white, lightgrey]),
    ('VALIGN', (0, -1), (-1, -1), 'TOP'),      
    ('LEFTPADDING', (0, 1), (-1, -2), 0),
    ('RIGHTPADDING', (0, 0), (-2, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 0)
])

TwoIndicatorTableStyle = TableStyle([
    ('SPAN', (0, 0), (-2, 0)),  
    ('LINEBELOW', (0, 0), (-1, 0), .8, black),
    ('VALIGN', (0, 1), (-1, -2), 'MIDDLE'),      
    ('LINEBELOW', (0, -2), (-1, -2), .8, black),  
    ('SPAN', (0, -1), (-1, -1)),  
    ('LINEBELOW', (0, 1), (-1, 1), .2, gray),

    ('ROWBACKGROUNDS', (0, 2), (-1, 3), [white, lightgrey]),
    ('VALIGN', (0, -1), (-1, -1), 'TOP'),      
    ('LEFTPADDING', (0, 1), (-1, -2), 0),
    ('RIGHTPADDING', (0, 0), (-2, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 0)
])


SingleIndicatorTCondensedableStyle = TableStyle([
    ('SPAN', (0, 0), (-1, 0)),  
    ('LINEBELOW', (0, 0), (-1, 0), .8, black),
    ('VALIGN', (0, 1), (-1, -1), 'MIDDLE'),      
    ('LINEBELOW', (0, -1), (-1, -1), .8, black),  
    ('LINEBELOW', (0, 1), (-1, 1), .2, gray),
    ('LEFTPADDING', (0, 1), (-1, -1), 0),
    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 0)
])

TwoOrMoreIndicatorCondensedTableStyle = TableStyle([
    ('SPAN', (0, 0), (1, 0)),  
    ('SPAN', (2, 0), (-1, 0)),  
    ('LINEBELOW', (0, 0), (-1, 0), .8, black),
    ('VALIGN', (0, 1), (-1, -1), 'MIDDLE'),      
    ('LINEBELOW', (0, -1), (-1, -1), .8, black),  
    ('LINEBELOW', (0, 1), (-1, 1), .2, gray),

    ('ROWBACKGROUNDS', (0, 2), (-1, -1), [white, lightgrey]),
    ('LEFTPADDING', (0, 1), (-1, -1), 0),
    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 0)
])

TwoIndicatorCondensedTableStyle = TableStyle([
    ('SPAN', (0, 0), (1, 0)),  
    ('SPAN', (2, 0), (-1, 0)),  
    ('LINEBELOW', (0, 0), (-1, 0), .8, black),
    ('VALIGN', (0, 1), (-1, -1), 'MIDDLE'),      
    ('LINEBELOW', (0, -1), (-1, -1), .8, black),  
    ('LINEBELOW', (0, 1), (-1, 1), .2, gray),

    ('ROWBACKGROUNDS', (0, 2), (-1, 3), [white, lightgrey]),
    ('LEFTPADDING', (0, 1), (-1, -1), 0),
    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 0)
])

Row1SpacerCondensedStyle = TableStyle([
    ('LINEBELOW', (0, 0), (-1, 0), .8, black),
    ('LINEBELOW', (0, -1), (-1, -1), .8, black),  
    ('LEFTPADDING', (0, 0), (-1, -1), 0),
    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 0),

])


ProgTypeIndicatorsAlignmentTableStyle = TableStyle([
    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
    ('LEFTPADDING', (0, 0), (-1, -1), 0),
    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
])

GlossaryTableStyle = TableStyle([
    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
    ('LINEABOVE', (0, 0), (-1, 0), .8, black),
    ('LINEBELOW', (0, -1), (-1, -1), .8, black),
    ('LINEABOVE', (0, 1), (-1, 1), .2, gray),
    ('LINEABOVE', (0, 2), (-1, 2), .2, gray),
    ('LINEABOVE', (0, 3), (-1, 3), .2, gray),
    ('LINEABOVE', (0, 4), (-1, 4), .2, gray),
    ('LINEABOVE', (0, 5), (-1, 5), .2, gray),
    
])

# Table Column and Row Formatting
agencyIndicatorsByDeptRegionColWidths = [inch*1.5, inch*.4, inch*.4, inch*.4, inch*.4, inch*.4, inch*.4]
agencyIndicatorsByDeptDeptColWidths = [inch*.4, inch*.4, inch*.4, inch*.4, inch*.4, inch*.4]
agencyIndicatorsByDeptAllRowHeights = inch/2.75
walkerGridColWidths = [.8*inch,1.8*inch,2.3*inch,.65*inch,.65*inch,.65*inch,.65*inch]
allIndicatorsRow1Height = [inch/4,inch/4,inch/4,inch/4,inch/4,inch/4,inch/4,inch/4]
allIndicatorsRow2Height = [inch/4,inch/4,inch/4,inch/4,inch/4,None]

progIndicatorsColWidths = [2.5*inch, .75*inch, .75*inch]

def ProgTypeIndicatorsRowHeights(rows):
    row_heights = []
    for r in range(rows):
        row_heights.append(inch/4)
    return row_heights

def ProgramLevelProgTypeIndicatorsRowWidth(num_of_indicators):
    row_heights =[]
    for indic in range(num_of_indicators+1):
        row_heights.append(18)
    return row_heights


def colummWidths(PrimaryDataSystem):
    if PrimaryDataSystem == "Vertical Change":
        return [inch,3*inch, inch, inch, inch,.5*inch]
    else:
        return [inch,3.5*inch, inch,.5*inch, inch,.5*inch]
