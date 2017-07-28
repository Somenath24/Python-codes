# -*- coding: utf-8 -*-
"""
Created on Thu Jul 27 12:53:17 2017

@author: Gramener
"""
from bs4 import BeautifulSoup
import urllib2
import os
import codecs
import string
import pandas as pd
import time
os.chdir("D:/External_data/Weather/")

######## Function for temp scrape ##########
def city_temp(url):
    req = urllib2.Request(url,headers=header)
    page = urllib2.urlopen(req)
    soup = BeautifulSoup(page)
    tables = soup.findAll("table", { "class" : "calendar-list" })
    table=tables[0]
    # preinit list of lists
    rows=table.findAll("tr")
    row_lengths=[len(r.findAll(['th','td'])) for r in rows]
    ncols=max(row_lengths)
    nrows=len(rows)
    data=[]
    for i in range(nrows):
        rowD=[]
        for j in range(ncols):
            rowD.append('')
        data.append(rowD)
    
    # process html
    for i in range(len(rows)):
        row=rows[i]
        rowD=[]
        cells = row.findAll(["td","th"])
        for j in range(len(cells)):
            cell=cells[j]
            
            #lots of cells span cols and rows so lets deal with that
            cspan=int(cell.get('colspan',1))
            rspan=int(cell.get('rowspan',1))
            for k in range(rspan):
                for l in range(cspan):
                    data[i+k][j+l]+=cell.text
    
        data.append(rowD)
        
    # write data out
    final=pd.DataFrame()
    for i in range(nrows):
        rowStr=','.join(data[i])
        rowStr=rowStr.replace('\n','')
        #print rowStr
        rowStr=rowStr#.encode('unicode_escape')
        rowStr=rowStr.encode('utf-8').strip() 
        rowStr=rowStr.decode('unicode_escape').encode('ascii','ignore')   
        test=pd.DataFrame(pd.Series(rowStr.split(",")))
        test=test.transpose()
        final=final.append(test)
    return(final)

############################################


url = "https://www.accuweather.com/en/browse-locations/asi/in"
header = {'User-Agent': 'Mozilla/5.0'} #Needed to prevent 403 error on Wikipedia
req = urllib2.Request(url,headers=header)
page = urllib2.urlopen(req)
soup = BeautifulSoup(page)

data = soup.findAll('div',attrs={'class':'info'})
state_urls=list()
for div in data:
    links = div.findAll('a')
    for a in links:
        state_urls.append(str(a['href']))
        print a['href']

del state_urls[-1]

for sturl in state_urls:
    print(sturl)
    req = urllib2.Request(sturl,headers=header)
    page = urllib2.urlopen(req)
    soup = BeautifulSoup(page)
    div_city = soup.findAll('div',attrs={'class':'info'})
    city_urls=list()
    for div in div_city:
        links = div.findAll('a')
        for a in links:
            city_urls.append(str(a['href']))
            print a['href']

    del city_urls[-1]



city_urls_temp = pd.DataFrame(city_urls)
city_urls_temp=pd.DataFrame(pd.Series(city_urls_temp[0].str.split("/")))

location_df = city_urls_temp.apply(lambda x: pd.Series(str(x).split('/')))
ctest=city_urls_temp[0].str.split("/")
cityid=list()
cityname=list()
rows=len(city_urls_temp)
for i in range(0,rows):
    cityid.append(city_urls_temp[0][i][6]) 
    cityname.append(city_urls_temp[0][i][5])

cityinfo=pd.DataFrame({'cityid':cityid,'cityname':cityname})
#citytemp="https://www.accuweather.com/en/in/"+cityinfo['cityname'][0]+"/"+cityinfo['cityid'][0]+"/month/"+ cityinfo['cityid'][0]+"?monyr=1/01/2017&view=table"

ncity=len(cityinfo)
datedf=pd.read_csv("dates.csv")
All_city_df=pd.DataFrame()
for i in range(0,ncity):
    ndates=len(datedf)
    city_df_temp=pd.DataFrame()
    city_date_temp=pd.DataFrame()
    for d in range(0,ndates):
        print(cityinfo['cityname'][i] + " " + datedf['Dates'][d]) 
        citytempurl="https://www.accuweather.com/en/in/"+cityinfo['cityname'][i]+"/"+cityinfo['cityid'][i]+"/month/"+ cityinfo['cityid'][i]+"?monyr="+datedf['Dates'][d]+"&view=table"
        time.sleep(0)        
        city_df=city_temp(citytempurl)
        city_df=city_df[city_df.columns[0:2]]
        city_df.columns=['Date','Hi-Lo']
        city_df=city_df.iloc[1:]
        city_date_sel=city_df[['Date']]
        city_date_temp=pd.concat([city_date_temp,city_date_sel])
        city_df_sel=city_df[['Hi-Lo']]
        city_df_sel.columns=[cityinfo['cityname'][i]]
        city_df_temp=pd.concat([city_df_temp,city_df_sel])
    time.sleep(2)
    All_city_df = pd.concat([All_city_df, city_df_temp], axis=1)
 
#### Write to files ##################
fname='All_temp.csv'
All_city_df.to_csv(fname, encoding='utf-8')
######################################