import os
import requests #Used to service API connection
# from lxml import html #Used to parse XML
from bs4 import BeautifulSoup #Used to read XML table on webpage
import pandas as pd
from pandas.io.json import json_normalize
#from tabula import read_pdf

def makeURL(myUrl, myKey, myParameter):
    # myUrl = "http://192.168.1.120/index.php?"
    url = myUrl + myKey + "&" + myParameter

    url = url.rstrip('&')
    return url

def xmlProcess(url):
    response = requests.get(url)
    # Check if page is up
    if response.status_code == 200:
        # Convert webpage to %Data
        Data = BeautifulSoup(response.text, 'lxml-xml')
        result = []
        rows = 0
        columnName = []
        # search Item all item tag
        iterData = Data.find_all('item')
        for item in iterData:
            item_list = []
            # Fill the value in one row
            for tag in item.find_all():
                try:
                    tagname = tag.name
                    if rows == 0:
                        columnName.append(tagname)
                    item_list.append(item.find(tagname).text)
                except Exception as e:
                    print("This row will be ignored. ", item_list)
            rows = rows + 1
            result.append(item_list)
    finalResult = pd.DataFrame(result)
    finalResult.columns = columnName
    print(finalResult)
    return finalResult


def operatorXmlProcess(url, bParameter):

    finalResult = pd.DataFrame()
    lastUrl = "";
    for i in range(0, 2):
        lastUrl = lastUrl + url.split('&')[i]
    parameter = ""
    for i in range(2, len(url.split('&'))):
        parameter = parameter + '&' + url.split('&')[i]
    parameterList = parameter.split('&')
    del parameterList[0]
    bpList = bParameter.split('&')
    regionCdData = pd.read_csv("../../data/infomations/RegionCode.csv")
    regionCdList = list(regionCdData["코드"])
    fisrtCount = True
    if len(bParameter) == 0:
        bParameter = "200601&201903&"
        bpList = bParameter.split('&')
        for i in range(0, len(parameterList)):
            lastUrl += '&' + (parameterList[i].split('=')[0] + '=' + bpList[i])
        for j in regionCdList:
            lastTotalUrl = lastUrl + str(j)
            newDF = xmlProcess(lastTotalUrl)
            rsRowList = newDF.loc[0, "rsRow"].split("|")
            for k in rsRowList:
                newDF[k.split(',')[0]] = k.split(',')[1]
            del newDF["rsRow"]
            if (fisrtCount):
                mergeData = newDF
                fisrtCount = False
            else:
                mergeData = pd.concat([mergeData, newDF], axis=0)
        groupKey = ["regionCd", "regionNm"]
        finalResult = pd.melt(mergeData, id_vars=groupKey, var_name="yearMonth", value_name="volume")
        finalResult = finalResult.sort_values(["regionCd", "regionNm"]).reset_index(drop=True)
    else:
        for l in range(0, len(parameterList)):
            lastUrl += '&' + (parameterList[l].split('=')[0] + '=' + bpList[l])
        newDF = xmlProcess(lastUrl)
        rsRowList = newDF.loc[0, "rsRow"].split("|")
        for k in rsRowList:
            newDF[k.split(',')[0]] = k.split(',')[1]
        del newDF["rsRow"]
        mergeData = newDF
        groupKey = ["regionCd", "regionNm"]
        finalResult = pd.melt(mergeData, id_vars=groupKey, var_name="yearMonth", value_name="volume")
        finalResult = finalResult.sort_values(["regionCd", "regionNm"]).reset_index(drop=True)

    return finalResult


def jsonProcess(url):

    response = requests.get(url)

    json = response.json()

    df = json_normalize(json)
    return df

def csvProcess(url):

    response = requests.get(url)

    df = pd.read_csv(url, encoding="ms949")
    return df

def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ('Error: Creating directory. ' +  directory)


def jsonProcess(url):

    response = requests.get(url)

    json = response.json()

    df = json_normalize(json)
    return df

def csvProcess(url):

    response = requests.get(url)

    df = pd.read_csv(url, encoding="ms949")
    return df

def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ('Error: Creating directory. ' +  directory)

def pdfProcess(url):

    response = requests.get(url)

    df = read_pdf("../../data/inbound/190402_금융시장동향.pdf", multiple_tables=True, pages="all")
    return df
