import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import date, timedelta

#Init dates variable
startDate = int(time.mktime(time.strptime('2016-01-01 00:00:00','%Y-%m-%d %H:%M:%S')))
currentDate = int(time.mktime(time.strptime(date.today().strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')))
endDate = startDate + 8553600

#Config header for htpp request
_header = {'User-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36'}


listofobj = []

while startDate < currentDate: #This loop is a workaround to the fact that the page only load 100 records per time and, the other records are loaded via Javascript

    #Set url with dynamic variables. Period 1 = Start date, Period 2 = End date
    _url = 'https://finance.yahoo.com/quote/BTC-USD/history?period1='+str(startDate)+'&period2='+str(endDate)+'&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true'
    
    r = requests.get(url = _url, headers = _header )
    soup = BeautifulSoup(r.text,'html.parser')

    #Find html table by class
    historicalPrices = soup.find('table',class_ = 'W(100%) M(0)')
    headTable = historicalPrices.thead.find_all("tr")

    # Get all the table headers
    headings = []
    for th in headTable[0].find_all("th"):
        headings.append(th.text.replace('\n', ' ').strip())

    # Get all the table body data
    tableData = []
    for tr in historicalPrices.tbody.find_all("tr"): #For each row
            tableRows = []
            
            for td in tr.find_all("td"): #For each cell in row
                tableRows.append(td.text.replace('\n', '').strip())
                
            tableData.append(tableRows)

    #Reverse the list to append it already sorted
    tableData = tableData[::-1]

    #Append the data as a dictionary
    listofobj += [dict(zip(headings, sublist)) for sublist in tableData]

    #Set the start date as the end date plus 1 days (1d=86400s)
    startDate = endDate + 86400

    #Set the end date as the end date plus 100 days (100d=8640000s)
    endDate = endDate + 8640000

#Construct a dataframe with the scrapped data
df = pd.DataFrame(data = listofobj)

#Export the dataframe to an excel file (.xls)
df.to_excel('Dataset.xls')
