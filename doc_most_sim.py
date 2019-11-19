#!/usr/bin/env python 
# -*- coding:utf-8 -*-
import os
import jieba
import ahocorasick
import math
import re
from sim_wordvector import *
import cx_Oracle
import pandas as pd
import datetime
from pyltp import Segmentor


class Doc_most_sim():
    def __init__(self):
        CUR_DIR = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        LTP_DIR = "D:\code\ltp_data_v3.4.0"
        DICT_DIR = os.path.join(CUR_DIR, 'data')
        LoaddictPath=os.path.join(DICT_DIR, 'dict.txt')
        CarnumberPath = os.path.join(DICT_DIR, 'public_number.txt')
        CompanyPath = os.path.join(DICT_DIR, 'car_series.txt')
        # ContentPath=os.path.join(DICT_DIR, 'content.txt')
        self.segmentor = Segmentor()
        self.segmentor.load(os.path.join(LTP_DIR, "cws.model"))
        self.segmentor.load_with_lexicon(os.path.join(LTP_DIR, "cws.model"), LoaddictPath)  # 加载模型，第二个参数是您的外部词典文件路径
        self.CarnumberList = [i.strip() for i in open(CarnumberPath,encoding='utf-8') if i.strip()]
        self.CompanyList = [i.strip() for i in open(CompanyPath,encoding='utf-8') if i.strip()]
        # self.ContentList=[i.strip() for i in open(ContentPath,encoding='utf-8') if len(i)>0]
        self.CarnumberTree = self.build_actree(self.CarnumberList)
        self.CompanyTree = self.build_actree(self.CompanyList)
        # self.UserWords = list(set(self.CarnumberList + self.CompanyList))
        # jieba.load_userdict(self.UserWords)
        self.simer = SimWordVec()

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

    def build_actree(self, wordlist):
        actree = ahocorasick.Automaton()
        for index, word in enumerate(wordlist):
            word = ' ' + word + ' '
            actree.add_word(word, (index, word))
        actree.make_automaton()
        return actree

    '''content分句处理'''

    def seg_sentences(self, content):
        return [content.replace('\u3000', '').replace('\xc2\xa0', '').replace(' ', '')]

    '''过滤'''

    def check_carnumber(self, sentence):
        flag = 0
        word_list = list(self.segmentor.segment(sentence))
        # print(word_list)
        senti_words = []
        for i in self.CarnumberTree.iter(' '.join(word_list + [' '])):
            senti_words.append(i[1][1].replace(' ', ''))
            flag += 1
        return flag, word_list, senti_words

    def check_company(self, sentence):
        flag = 0
        word_list = list(self.segmentor.segment(sentence))
        # print(word_list)
        senti_words = []
        for i in self.CompanyTree.iter(' '.join(word_list + [' '])):
            senti_words.append(i[1][1].replace(' ', ''))
            flag += 1
        return flag, word_list, senti_words

    '''提取最相似内容'''

    def extract_most_sim_content(self, sentences):
        # senti_sentences = list()
        sims=[]
        for index, sentence in enumerate(sentences):
            flag, word_list, senti_words = self.check_carnumber(sentence)
            senti_words=list(set(senti_words))
            if flag:
                user = 'dd_data'
                password = 'xdf123'
                database = 'LBORA170'
                targetTable = 'soure_01'
                # commandText = '''select t.STD_MODEL_INFO from CS_JY_2 t where t.GGID='{}' '''.format(senti_words[0])
                commandText1 = '''select t.STD_MODEL_INFO from  A_MODEL_20190617 t where t.GGID='{}' '''.format(senti_words[0])
                # cs_jy_data = self.getData(user, password, database, targetTable, commandText)
                cs_cb_data = self.getData(user, password, database, targetTable, commandText1)
                id2content={}
                sim2id={}
                # for i in range(len(cs_jy_data)):
                #     content=str(cs_jy_data['STD_MODEL_INFO'][i])
                #     id2content[len(id2content)]=content
                #     cn = list(self.segmentor.segment(content.replace('\r', '').replace(' ', '').replace('\u3000', '')))
                #     sim=(self.simer.distance(word_list, cn))
                #     sim2id[sim]=len(sim2id)
                #     sims.append(sim)

                for j in range(len(cs_cb_data)):
                    content = str(cs_cb_data['STD_MODEL_INFO'][j])
                    id2content[len(id2content)] = content
                    cn = list(self.segmentor.segment(content.replace('\r', '').replace(' ', '').replace('\u3000', '')))
                    sim = (self.simer.distance(word_list, cn))
                    sim2id[sim] = j
                    sims.append(sim)
                # print(id2content)
                # print(sim2id)

            else:
                flag, word_list, senti_words = self.check_company(sentence)
                senti_words = list(set(senti_words))
                if flag:
                    id2content = {}
                    sim2id = {}
                    for k in range(len(senti_words)):
                        user = 'dd_data'
                        password = 'xdf123'
                        database = 'LBORA170'
                        targetTable = 'soure_01'
                        # commandText = '''select t.STD_MODEL_INFO from CS_JY_2 t where t.CHINESE_CS='{}' '''.format(senti_words[k])
                        commandText1 = '''select t.STD_MODEL_INFO from  A_MODEL_20190617 t where t.MODEL_CATE_NAME='{}' '''.format(senti_words[k])
                        # cs_jy_data = self.getData(user, password, database, targetTable, commandText)
                        cs_cb_data = self.getData(user, password, database, targetTable, commandText1)

                        # for i in range(len(cs_jy_data)):
                        #     content = str(cs_jy_data['STD_MODEL_INFO'][i])
                        #     id2content[len(id2content)] = content
                        #     cn = list(self.segmentor.segment(content.replace('\r', '').replace(' ', '').replace('\u3000', '')))
                        #     sim = (self.simer.distance(word_list, cn))
                        #     sim2id[sim] = len(sim2id)
                        #     sims.append(sim)

                        for j in range(len(cs_cb_data)):
                            content = str(cs_cb_data['STD_MODEL_INFO'][j])
                            id2content[len(id2content)] = content
                            cn = list(self.segmentor.segment(content.replace('\r', '').replace(' ', '').replace('\u3000', '')))
                            sim = (self.simer.distance(word_list, cn))
                            sim2id[sim] = j
                            sims.append(sim)


                else:
                    return '您输入的内容必须包含公众号或车系，否则无法匹配到您需要的车型！抱歉'
                    # user = 'dd_data'
                    # password = 'xdf123'
                    # database = 'LBORA170'
                    # targetTable = 'soure_01'
                    # commandText = '''select t.STD_MODEL_INFO from CS_JY_2 t '''
                    # commandText1 = '''select t.STD_MODEL_INFO from  A_MODEL_20190617 t where rownum<11'''
                    # cs_jy_data = self.getData(user, password, database, targetTable, commandText)
                    # cs_cb_data = self.getData(user, password, database, targetTable, commandText1)
                    # id2content = {}
                    # sim2id = {}
                    # for i in range(len(cs_jy_data)):
                    #     content = str(cs_jy_data['STD_MODEL_INFO'][i])
                    #     id2content[i] = content
                    #     cn = ''.join(content.split(' '))
                    #     cn = ' '.join(jieba.cut(cn))
                    #     simer = SimWordVec()
                    #     sim = (simer.distance(word_list, cn))
                    #     sim2id[sim] = i
                    #     sims.append(sim)
                    #
                    # for j in range(len(cs_jy_data), len(cs_jy_data) + len(cs_cb_data)):
                    #     content = str(cs_cb_data['STD_MODEL_INFO'][i])
                    #     id2content[j] = content
                    #     cn = ''.join(content.split(' '))
                    #     cn = ' '.join(jieba.cut(cn))
                    #     simer = SimWordVec()
                    #     sim = (simer.distance(word_list, cn))
                    #     sim2id[sim] = j
                    #     sims.append(sim)
        try:
            max_sim=max(sims)
            result=id2content[sim2id[max_sim]]
            return max_sim,result
        except :
            pass




    def doc_score(self, content):
        sents = self.seg_sentences(content)
        max_sim,result_content = self.extract_most_sim_content(sents)
        return max_sim,result_content

if __name__=='__main__':
    content6='东风DFH5160XXYEX3B厢式运输车东风天锦货车东风东风天锦KR3东风汽车发动机型号:福田康明斯ISD180 62;底盘型号:东风DFH1160E3载重货车东风DFH5160XXYEX3B厢式运输车东风DFH5160XXYEX3B厢式运输车东风'
    content1='龍軒風YLT9401GD龍軒風半挂车龍軒風龍軒風YLT94015龍軒風YLT9401GD大众龍軒風'
    content2='豪门HM48Q-6A两轮轻便摩托车摩托车摩托车摩托车车组（5千以下）1摩托车生产厂家摩托车豪门HM48Q-6A两轮轻便摩托车豪门HM48Q-6A两轮轻便摩托车摩托车'
    handler = Doc_most_sim()
    start_time = datetime.datetime.now()
    sim_max, result_content=handler.doc_score(content1)
    end_time = datetime.datetime.now()
    print(sim_max,result_content)
    print('总耗时：%.1f秒' % ((end_time - start_time).seconds))
    # with open('data/test.txt','w',encoding='utf-8') as writer:
    #     for line in open('data/fsdfs.csv','r',encoding='utf-8'):
    #         sim_max,result_content = handler.doc_score(line)
    #         writer.write(str(sim_max)+'\t'+str(result_content)+'\n')

