from dateutil import parser

def convertDaysToTimestamp(days):
    return days*24*60*60

def convertToTimestamp(date):
    if date == "":
        date = '2016-01-01 00:00:00'
    
    normalizedDate = parser.parse(date)
    return int(normalizedDate.timestamp())