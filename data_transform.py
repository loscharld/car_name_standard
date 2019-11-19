#!/usr/bin/env python 
# -*- coding:utf-8 -*-

import pandas as pd
datas=pd.read_excel('data/汽车之家现售_.xlsx')
print(datas.head())
datas['车系']=datas.apply(lambda x:x['车系'].upper(),axis=1)
print(datas.head())
datas.to_excel('data/car_family.xlsx',index=None,encoding='utf-8')