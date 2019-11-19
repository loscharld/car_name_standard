#!/usr/bin/env python 
# -*- coding:utf-8 -*-
import os
import jieba
import ahocorasick
import math
import re
import cx_Oracle
import pandas as pd
import gensim, logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
import numpy as np
import jieba.posseg as pesg
from pyltp import Segmentor
import datetime

class SimWordVec:
    def __init__(self):
        self.embedding_path = 'model/skipgram_wordvec.bin'
        self.model = gensim.models.KeyedVectors.load_word2vec_format(self.embedding_path, binary=False)
    '''获取词向量'''
    def get_wordvector(self, word):#获取词向量
        try:
            return self.model[word]
        except:
            return np.zeros(200)
    '''基于余弦相似度计算句子之间的相似度，句子向量等于字符向量求平均'''
    def similarity_cosine(self, word_list1,word_list2):#给予余弦相似度的相似度计算
        vector1 = np.zeros(200)
        for word in word_list1:
            vector1 += self.get_wordvector(word)
        vector1=vector1/len(word_list1)
        vector2=np.zeros(200)
        for word in word_list2:
            vector2 += self.get_wordvector(word)
        vector2=vector2/len(word_list2)
        cos1 = np.sum(vector1*vector2)
        cos21 = np.sqrt(sum(vector1**2))
        cos22 = np.sqrt(sum(vector2**2))
        similarity = cos1/float(cos21*cos22)
        return  similarity
    '''计算句子相似度'''
    def distance(self, text1, text2):#相似性计算主函数
        word_list1=[word for word in text1 if len(word)<10]
        word_list2=[word for word in text2 if len(word)<10]
        return self.similarity_cosine(word_list1,word_list2)

class Doc_most_sim():
    def __init__(self):
        CUR_DIR = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        LTP_DIR = "D:\code\ltp_data_v3.4.0"
        DICT_DIR = os.path.join(CUR_DIR, 'data')
        # LoaddictPath=os.path.join(DICT_DIR, 'dict.txt')
        CarnumberPath = os.path.join(DICT_DIR, 'car_series.txt')
        CompanyPath = os.path.join(DICT_DIR, 'brand.txt')
        # ContentPath=os.path.join(DICT_DIR, 'content.txt')
        # self.segmentor = Segmentor()
        # self.segmentor.load(os.path.join(LTP_DIR, "cws.model"))
        # self.segmentor.load_with_lexicon(os.path.join(LTP_DIR, "cws.model"), CarnumberPath)  # 加载模型，第二个参数是您的外部词典文件路径
        self.CarnumberList = [i.strip() for i in open(CarnumberPath,encoding='utf-8') if i.strip()]
        self.CompanyList = [i.strip() for i in open(CompanyPath,encoding='utf-8') if i.strip()]
        # self.ContentList=[i.strip() for i in open(ContentPath,encoding='utf-8') if len(i)>0]
        self.CarnumberTree = self.build_actree(self.CarnumberList)
        self.CompanyTree = self.build_actree(self.CompanyList)
        self.UserWords = list(set(self.CarnumberList ))
        jieba.load_userdict(self.UserWords)
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
        word_list = list(jieba.cut(sentence))
        # print(word_list)
        senti_words = []
        for i in self.CarnumberTree.iter(' '.join(word_list + [' '])):
            senti_words.append(i[1][1].replace(' ', ''))
            flag += 1
        return flag, word_list, senti_words

    def check_company(self, sentence):
        flag = 0
        word_list = list(jieba.cut(sentence))
        # print(word_list)
        senti_words = []
        for i in self.CompanyTree.iter(' '.join(word_list + [' '])):
            senti_words.append(i[1][1].replace(' ', ''))
            flag += 1
        return flag, word_list, senti_words

    '''提取最相似内容'''

    def extract_most_sim_content(self, sentences,ff):
        # senti_sentences = list()
        sims=[]
        user = 'VDATA'
        password = 'xdf123'
        database = 'LBORA170'
        targetTable = 'soure_01'
        for index, sentence in enumerate(sentences):
            flag, word_list, senti_words = self.check_carnumber(sentence)
            senti_words=list(set(senti_words))
            if flag:

                # commandText = '''select t.STD_MODEL_INFO from CS_JY_2 t where t.GGID='{}' '''.format(senti_words[0])
                # time1=datetime.datetime.now()
                commandText1 = '''select t.STD_MODEL_INFO from  a_model t where t.MODEL_CATE_NAME='{}' '''.format(senti_words[0])
                # cs_jy_data = self.getData(user, password, database, targetTable, commandText)
                cs_cb_data = self.getData(user, password, database, targetTable, commandText1)
                # time2=datetime.datetime.now()
                # print('读取表内容所花时间：%s'%(time2-time1).seconds)
                id2content={}
                sim2id={}
                # for i in range(len(cs_jy_data)):
                #     content=str(cs_jy_data['STD_MODEL_INFO'][i])
                #     id2content[len(id2content)]=content
                #     cn = list(jieba.cut(content.replace('\r', '').replace(' ', '').replace('\u3000', '')))
                #     sim=(self.simer.distance(word_list, cn))
                #     sim2id[sim]=len(sim2id)
                #     sims.append(sim)

                for j in range(len(cs_cb_data)):
                    content = str(cs_cb_data['STD_MODEL_INFO'][j])
                    id2content[len(id2content)] = content
                    cn = list(jieba.cut(content.replace('\r', '').replace(' ', '').replace('\u3000', '')))
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

                        # commandText = '''select t.STD_MODEL_INFO from CS_JY_2 t where t.CHINESE_CS='{}' '''.format(senti_words[k])
                        commandText1 = '''select t.STD_MODEL_INFO from  a_model t where t.MODEL_BRAND='{}' '''.format(senti_words[k])
                        # cs_jy_data = self.getData(user, password, database, targetTable, commandText)
                        cs_cb_data = self.getData(user, password, database, targetTable, commandText1)

                        # for i in range(len(cs_jy_data)):
                        #     content = str(cs_jy_data['STD_MODEL_INFO'][i])
                        #     id2content[len(id2content)] = content
                        #     cn = list(jieba.cut(content.replace('\r', '').replace(' ', '').replace('\u3000', '')))
                        #     sim = (self.simer.distance(word_list, cn))
                        #     sim2id[sim] = len(sim2id)
                        #     sims.append(sim)

                        for j in range(len(cs_cb_data)):
                            content = str(cs_cb_data['STD_MODEL_INFO'][j])
                            id2content[len(id2content)] = content
                            cn = list(jieba.cut(
                                content.replace('\r', '').replace(' ', '').replace('\u3000', '')))
                            sim = (self.simer.distance(word_list, cn))
                            sim2id[sim] = j
                            sims.append(sim)


                else:
                    return'您输入的内容必须包含公众号或车系，否则无法匹配到您需要的车型！抱歉'
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
            if float(max_sim)>0.9:
                result=id2content[sim2id[max_sim]]
                # commandText1 = '''select * from  cs_cb_handing t where t.STD_MODEL_INFO='{}' '''.format(result)
                # cs_cb_data1 = self.getData(user, password, database, targetTable, commandText1)
                # info={}
                # if len(cs_cb_data1):
                #     GB1=str(cs_cb_data1['GB1'][0])
                #     ENERGY_TYPE=str(cs_cb_data1['ENERGY_TYPE'][0])
                #     HORSE_POWER=str(cs_cb_data1['HORSE_POWER'][0])
                #     TYPE=str(cs_cb_data1['TYPE'][0])
                #     CAR_TYPE=str(cs_cb_data1['CAR_TYPE'][0])
                #     GEARBOX=str(cs_cb_data1['GEARBOX'][0])
                #     DOORS=str(cs_cb_data1['DOORS'][0])
                #     STD_MODEL_INFO=str(cs_cb_data1['STD_MODEL_INFO'][0])
                #
                #     info['GB1'] = GB1
                #     info['ENERGY_TYPE'] = ENERGY_TYPE
                #     info['HORSE_POWER'] = HORSE_POWER
                #     info['TYPE'] = TYPE
                #     info['CAR_TYPE'] = CAR_TYPE
                #     info['GEARBOX'] = GEARBOX
                #     info['DOORS'] = DOORS
                #     info['STD_MODEL_INFO']=STD_MODEL_INFO
                #
                #     return info
                # time3=datetime.datetime.now()
                # print('最大相似度获取所花时间%s'%(time3-time2).seconds)
                commandText='''Merge into {} s Using {} nn On (s.STD_MODEL_INFO='{}' and nn.STD_MODEL_INFO='{}')
                When matched then update set s.MODEL_ID = nn.MODEL_ID
               '''.format('qczj_0722','a_model',ff,result)


                self.upData(commandText)
                # time4=datetime.datetime.now()
                # print('更新表所花时间%s'%(time4-time3).seconds)
                return
        except Exception as e:
            print(e)

    def getData1(self,commmat):
        connection = cx_Oracle.connect("VDATA", "xdf123", "LBORA170")
        cursor = connection.cursor()
        cursor.execute(commmat)
        x = cursor.description
        columns = [y[0] for y in x]
        cursor01 = cursor.fetchall()
        cursor.close()
        data = pd.DataFrame(cursor01, columns=columns)
        return data

    def upData(self,commat):
        connection = cx_Oracle.connect("VDATA", "xdf123", "LBORA170")
        cursor = connection.cursor()
        cursor.execute(commat)
        connection.commit()
        cursor.close()
        connection.close()
        return


    def doc_score(self, content):
        sents = self.seg_sentences(content)
        result_content = self.extract_most_sim_content(sents,content)
        return result_content

if __name__=='__main__':
    targetTable1 = 'qczj_0722'
    commmat1 = "SELECT * FROM {}".format(targetTable1)
    handle=Doc_most_sim()
    qczj_0722 = handle.getData1(commmat1)
    for i in range(len(qczj_0722)):
        content = qczj_0722['STD_MODEL_INFO'][i]
        start_time = datetime.datetime.now()
        handle.doc_score(content)
        end_time = datetime.datetime.now()
        s=str(i)+'\t'+str((end_time - start_time).seconds)
        print(s)

