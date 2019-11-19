#!/usr/bin/env python 
# -*- coding:utf-8 -*-
import cx_Oracle
import pandas as pd


def getData(user, password, database, targetTable, commandText):
    connection = cx_Oracle.connect(user, password, database)
    cursor = connection.cursor()
    cursor.execute(commandText.format(targetTable))
    x = cursor.description
    columns = [y[0] for y in x]
    cursor01 = cursor.fetchall()
    cursor.close()
    data = pd.DataFrame(cursor01, columns=columns)
    return data


def get_dict(input_path):
    # contents=[]
    public_numbers=[]
    car_serieses=[]
    brands=[]
    for i,line in enumerate(open(input_path,'r',encoding='utf-8')):
        try:
            # content=line.strip().split('#')[0].split(' ')
            public_number=line.strip().split('#')[1].replace('\u3000', '').replace('\xc2\xa0', '').replace(' ', '')
            car_series=line.strip().split('#')[-2].strip().upper().replace('\u3000', '').replace('\xc2\xa0', '').replace(' ', '')
            brand=line.strip().split('#')[-1].strip().upper().replace('\u3000', '').replace('\xc2\xa0', '').replace(' ', '')
            # contents.extend([i for i in content if len(i)>0 and len(i)<30])
            if public_number and 'nan' not in public_number:
                public_numbers.append(public_number)
            if car_series and 'nan' not in car_series and len(car_series)<7:
                car_serieses.append(car_series)

            if brand and 'nan' not in brand:
                brands.append(brand)
        except:
            print(i,line)

    public_numbers=sorted(list(set(public_numbers)),key=lambda x:len(x),reverse=True)
    car_serieses=sorted(list(set(car_serieses)),key=lambda x:len(x),reverse=True)
    brands=sorted(list(set(brands)),key=lambda x:len(x),reverse=True)
    return public_numbers,car_serieses,brands




if __name__=='__main__':
    user = 'dd_data'
    password = 'xdf123'
    database = 'LBORA170'
    targetTable = 'soure_01'
    commandText = '''select t.STD_MODEL_INFO,t.GGID,t.CHINESE_CS,t.BRAND_P from CS_JY_2 t'''

    train_data = getData(user, password, database, targetTable, commandText)
    train_data.to_csv('data/standard1.txt',index=None,encoding='utf-8',header=None,sep='#')
    input_path='data/standard1.txt'
    public_numbers,car_serieses,brands=get_dict(input_path)
    dict=list(set(car_serieses+brands))
    f1=open('data/dict.txt','w',encoding='utf-8')
    f2=open('data/car_series_c.txt','w',encoding='utf-8')
    f3=open('data/public_number.txt','w',encoding='utf-8')
    f4=open('data/brand.txt','w',encoding='utf-8')
    f1.write('\n'.join(dict))
    f2.write('\n'.join(car_serieses))
    f3.write('\n'.join(public_numbers))
    f4.write('\n'.join(brands))
    f1.close()
    f2.close()
    f3.close()
    f4.close()