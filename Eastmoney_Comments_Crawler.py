# -*- coding: utf-8 -*-
"""
Created on Thu Aug 22 10:55:52 2019

"""

from bs4 import BeautifulSoup
import requests
import pandas as pd
#import numpy as np

import time
from multiprocessing.dummy import Pool as ThreadPool
import os

def run(num):
    if not os.path.exists('E:\\qtdata\\eastmoney\\'+str(num)+'.csv'):
        url = 'http://guba.eastmoney.com/list,'+str(num)

        result = pd.DataFrame(columns = ['time','read','comment','contents'])
        order = 0
        k = 1
        while 1:            
            url_temp = url+'_'+str(k)+'.html'
            k += 1
            r = requests.get(url_temp)
            demo = r.text
            soup = BeautifulSoup(demo, "html.parser")
            reads = soup.find_all('span',class_="l1 a1")
            attempts = 0
            success = False
            if len(reads) < 2:
                while attempts < 10 and not success:
                    r = requests.get(url_temp)
                    demo = r.text
                    soup = BeautifulSoup(demo, "html.parser")
                    reads = soup.find_all('span',class_="l1 a1")
                    if len(reads) >= 2:
                        success = True
                    else:
                        attempts += 1
                        if attempts <= 5:
                            time.sleep(1)
                        elif attempts <= 8 :
                            time.sleep(3)
                        else:
                            time.sleep(5)
            if attempts > 8:
                break
            comments = soup.find_all('span',class_="l2 a2")
            contents = soup.find_all('span',class_="l3 a3")
            times = soup.find_all('span',class_="l5 a5")
            r.keep_alive = False
            time1 = time.time()
            if len(contents) == len(comments):
                for i in range(len(contents)):
                    try:
                        content =contents[i].find('a')
                        read = reads[i].text
                        comment = comments[i].text
                        time_ = times[i].text
                        if content is not None:
                            order += 1
                            result.loc[order] = [time_,read, comment ,content['title']]
                    except KeyError as e:
                        if str(e) == "'title'":
                            content = contents[i].find_all('a')[1]
                            read = reads[i].text
                            comment = comments[i].text
                            time_ = times[i].text
                            if content is not None:
                                order += 1
                                result.loc[order] = [time_,read, comment ,content['title']]
                    except BaseException as e:
                        print(e)
            else:
                j = 0
                for i in range(len(comments)):
                    try:
                        content =contents[j].find('a')
                        read = reads[i].text
                        comment = comments[i].text
                        time_ = times[i].text
                        if content is not None:
                            order += 1
                            result.loc[order] = [time_,read, comment ,content['title']]
                        if contents[j].find('em',class_ ="ask") is not None:
                            j += 1
                    except KeyError as e:
                        if str(e) == "'title'":
                            content = contents[j].find_all('a')[1]
                            read = reads[i].text
                            comment = comments[i].text
                            time_ = times[i].text
                            if content is not None:
                                order += 1
                                result.loc[order] = [time_,read, comment ,content['title']]
                            if contents[j].find('em',class_ ="ask") is not None:
                                j += 1  
                    except BaseException as e:
                        print(e)                                
                    j += 1
            time2 = time.time()
            try:
                print(str(num),str(k-1),time_,read,comment,content['title'],str(time2-time1),' s')
            except BaseException as e:
                print(e)
        result.to_csv("E:\qtdata\eastmoney\\"+num+".csv")
if __name__ == "__main__":
    thread_n = int(input("Num Thread:"))
    pool = ThreadPool(thread_n)
    filePath = 'E:\\qtdata\\eastmoney\\'
    file_list = os.listdir(filePath)
    company_list = list(pd.read_csv('E:\\py\\crawler\\share_code.txt',header = None)[0])
    companys = []
    files = []
    for i in company_list:
        companys.append(i[:-3])   
    for j in file_list:
        files.append(j[:-4])     
    diff = list(set(companys)-set(files))
    diff.sort()
    num = []
    start = int(input("start:"))
    end = int(input("end:"))
    for k in diff[start:end]:
        num.append(k)
    results = pool.map(run, num)
    pool.close()
    pool.join()
