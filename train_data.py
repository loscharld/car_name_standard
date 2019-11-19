#!/usr/bin/env python 
# -*- coding:utf-8 -*-

import os
import jieba
import pkuseg
def get_train_data(input_path,out_path):
    CUR_DIR = '/'.join(os.path.abspath(__file__).split('/')[:-1])
    DICT_DIR = os.path.join(CUR_DIR, 'data1')
    ChexiPath = os.path.join(DICT_DIR, 'car_series_jingyou_all.txt')
    Dict_path=os.path.join(DICT_DIR, 'dict_all.txt')
    BanxingPath = os.path.join(DICT_DIR, 'banxing.txt')
    ChexiList = [i.strip() for i in open(ChexiPath, encoding='utf-8') if i.strip()]
    # CompanyList = [i.strip() for i in open(CompanyPath, encoding='utf-8') if i.strip()]
    Dict_list=[i.strip() for i in open(Dict_path, encoding='utf-8') if i.strip()]
    # UserWords = list(set(CarnumberList + CompanyList))
    seg = pkuseg.pkuseg(user_dict=Dict_list)

    with open(out_path,'w',encoding='utf-8') as writer:
        for i,line in enumerate(open(input_path,'r',encoding='utf-8')):
            try:
                content=line.strip().split('#')[0].replace('Ⅳ','IV').replace('Ⅴ','V').replace('Ⅵ','VI').replace('Ⅲ','III').replace('国五','国V').replace('国二','国II')
                if content:
                    content=' '.join(seg.cut(content.replace('\r', '').replace(' ','').replace('\u3000', '')))
                    writer.write(content+'\n')
            except:
                print(i)

if __name__=='__main__':
    input_path='data/standard.txt'
    output_path='data1/train_data.txt'
    get_train_data(input_path, output_path)
