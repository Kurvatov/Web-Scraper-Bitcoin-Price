from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta, date
import httpx
import asyncio
import requests
import json
from app.helpers import *
import os
from dotenv import load_dotenv

load_dotenv()
ENV = os.getenv("ENV", default="dev")
API_KEY = os.getenv("API_KEY")

def getLastDate():
    req = requests.get('https://btc-stats.herokuapp.com/stats?limit=1&date_order=desc', headers={'access_token': API_KEY})
    response = json.loads(req.text)

    if response['items'] == []:
        return ""
    
    normalizedDate = parser.parse(response['items'][0]['timestamp'])
    
    if  (normalizedDate + timedelta(1)) < datetime.today():
        return (normalizedDate + timedelta(1))
    else:
        return None


def buildUrls():
    _startDate = getLastDate()
    
    if _startDate == None:
        return None
    #Init dates variable
    startDate = convertToTimestamp(str(_startDate))
    currentDate = convertToTimestamp(str(datetime.today()))

    endDate = startDate + convertDaysToTimestamp(99) #To get the date I increment the start date by 99 days
    #Create list of URL's for async calls
    urls = []
    while startDate < currentDate:
        _url = 'https://finance.yahoo.com/quote/BTC-USD/history?period1='+str(startDate)+'&period2='+str(endDate)+'&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true'
        urls.append(_url)

        #Set the start date as the end date plus 1 days (1d=86400s)
        startDate = endDate + convertDaysToTimestamp(1)

        #Set the end date as the end date plus 100 days (100d=8640000s)
        endDate = endDate + convertDaysToTimestamp(100)
    
    return urls

async def getAllData():
    urls = buildUrls()

    if urls == None:
        return None

    
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
        df.rename(columns={'Date': 'timestamp', 'Open': 'open', 'Close*': 'close', 'High': 'higher', 'Low': 'lower', 'Volume': 'volume'}, inplace=True)
        del df['Adj Close**']
        df['open'] = df['open'].str.replace(',', '')
        df['close'] = df['close'].str.replace(',', '')
        df['higher'] = df['higher'].str.replace(',', '')
        df['lower'] = df['lower'].str.replace(',', '')
        df['volume'] = df['volume'].str.replace(',', '')

        df['open'] = df['open'].str.replace('-', '')
        df['close'] = df['close'].str.replace('-', '')
        df['higher'] = df['higher'].str.replace('-', '')
        df['lower'] = df['lower'].str.replace('-', '')
        df['volume'] = df['volume'].str.replace('-', '')
        

        df.replace("", None, inplace=True)

        df.dropna(inplace=True)
        jsonObject = df.to_json(orient='records')
        return jsonObject
        
def runAsyncio(): 
    return asyncio.run(getAllData())

def bulkPost():
    jsonObject = runAsyncio()
    if jsonObject == None:
        return None
    return requests.post('https://btc-stats.herokuapp.com/stats', headers={'access_token': API_KEY},data=jsonObject)




