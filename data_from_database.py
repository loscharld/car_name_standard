#!/usr/bin/env python 
# -*- coding:utf-8 -*-
import cx_Oracle
import pandas as pd
import datetime
import os

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





if __name__=='__main__':
    user = 'dd_data'
    password = 'xdf123'
    database = 'LBORA170'
    targetTable = 'soure_01'
    commandText = '''select t.std_model_together_add from XS_CAR_QUCHONG t'''
    # commandText1 = '''select t.STD_MODEL_INFO,t.GGID,t.MODEL_CATE_NAME from  A_MODEL_20190617 t'''

    train_data = getData(user, password, database, targetTable, commandText)
    # train_data1 = getData(user, password, database, targetTable, commandText1)
    # train_data['MODEL_CATE_NAME'] = train_data['CHINESE_CS']
    # train_data.drop('CHINESE_CS', axis=1, inplace=True)
    # train_data_all=pd.concat([train_data,train_data1],axis=0)
    train_data.to_csv('data/standard.txt', index=None, header=None,encoding='utf-8',sep='#')
