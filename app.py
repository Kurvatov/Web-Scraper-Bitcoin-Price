from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import date
import httpx
import asyncio

#Init dates variable
startDate = int(time.mktime(time.strptime('2016-01-01 00:00:00','%Y-%m-%d %H:%M:%S')))
currentDate = int(time.mktime(time.strptime(date.today().strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')))
endDate = startDate + 8553600 #To get the date I increment the start date by 99 days

#Create list of URL's for async calls
urls = []
while startDate < currentDate:
    _url = 'https://finance.yahoo.com/quote/BTC-USD/history?period1='+str(startDate)+'&period2='+str(endDate)+'&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true'
    urls.append(_url)

    #Set the start date as the end date plus 1 days (1d=86400s)
    startDate = endDate + 86400

    #Set the end date as the end date plus 100 days (100d=8640000s)
    endDate = endDate + 8640000

async def getAllData():
    listofobj = []

    async with httpx.AsyncClient() as client:
        tasks = (client.get(url) for url in urls)
        reqs = await asyncio.gather(*tasks)

        for r in reqs:
            soup = BeautifulSoup(r.text,'html.parser')
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
        df = pd.DataFrame(data = listofobj)
        

asyncio.run(getAllData())

