#!/usr/bin/env python 
# -*- coding:utf-8 -*-

import pymongo
import cx_Oracle
import pandas as pd

class IntoMongo:
    def __init__(self):
        conn = pymongo.MongoClient()
        self.db = conn['A_MODEL_20190617']['data']

    def getData(self,user, password, database, targetTable, commandText):
        connection = cx_Oracle.connect(user, password, database)
        cursor = connection.cursor()
        cursor.execute(commandText.format(targetTable))
        x = cursor.description
        columns = [y[0] for y in x]
        cursor01 = cursor.fetchall()
        cursor.close()
        data = pd.DataFrame(cursor01, columns=columns)
        return data


    def walk(self,train_data):
        for i in range(len(train_data)):
            try:
                data = self.case_parser(train_data1, i)
                if data:
                    try:
                        self.db.insert_one(data)
                    except Exception as e:
                        print(e)
                        print('duplicated...')
            except:
                pass

        return


    def case_parser(self, block,index):
        data = {}
        model_id=str(block['MODEL_ID'][index])
        model_standard_name = str(block['MODEL_STANDARD_NAME'][index])
        model_factory = str(block['MODEL_FACTORY'][index])
        model_brand = str(block['MODEL_BRAND'][index])
        model_cate_name = str(block['MODEL_CATE_NAME'][index])
        std_model_info = str(block['STD_MODEL_INFO'][index])
        ggid = str(block['GGID'][index])
        table_name = 'A_MODEL_20190617'
        data['model_id'] = model_id
        data['model_standard_name'] = model_standard_name
        data['model_factory'] =model_factory
        data['model_brand'] = model_brand
        data['model_cate_name'] = model_cate_name
        data['std_model_info'] = std_model_info
        data['ggid'] = ggid
        data['table_name'] = table_name
        return data

    '''规范化时间表示'''
    def time_modify(self, time):
        year = time.split('-')[0]
        month = time.split('-')[1]
        day = time.split('-')[2]
        if int(month) < 10 and len(month) < 2:
            month = '0' + month

        if int(day) < 10 and len(day) < 2:
            day = '0' + day

        return '-'.join([year, month, day])




if __name__=='__main__':
    user = 'dd_data'
    password = 'xdf123'
    database = 'LBORA170'
    targetTable = 'soure_01'
    # commandText = '''select t.STD_MODEL_INFO,t.GGID,t.CHINESE_CS from CS_JY_2 t'''
    commandText1 = '''select t.model_id, t.model_standard_name,  t.model_factory , t.model_brand,t.model_cate_name,t.STD_MODEL_INFO,t.GGID from  A_MODEL_20190617 t'''
    handle=IntoMongo()
    # train_data = handle.getData(user, password, database, targetTable, commandText)
    train_data1 =handle.getData(user, password, database, targetTable, commandText1)
    handle.walk(train_data1)



    # train_data['MODEL_CATE_NAME'] = train_data['CHINESE_CS']
    # train_data.drop('CHINESE_CS', axis=1, inplace=True)
    # train_data_all=pd.concat([train_data,train_data1],axis=0)
    # train_data_all.to_csv('data/standard.txt', index=None, header=None,encoding='utf-8',sep='#')
