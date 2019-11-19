
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
class ThreadXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass
import os
import jieba
import ahocorasick
import math
import re
from sim_wordvector import *
import cx_Oracle
import pandas as pd
import gensim, logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
import numpy as np
import jieba.posseg as pesg
from pyltp import Segmentor

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
        # jieba.load_userdict(LoaddictPath)
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

    '''content处理'''

    def seg_sentences(self, content):
        return [content.replace('\u3000', '').replace('\xc2\xa0', '').replace(' ', '')]

    '''匹配关键词'''

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
                #     cn = list(jieba.cut(content.replace('\r', '').replace(' ', '').replace('\u3000', '')))
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
                        targetTable = ''
                        # commandText = '''select t.STD_MODEL_INFO from CS_JY_2 t where t.CHINESE_CS='{}' '''.format(senti_words[k])
                        commandText1 = '''select t.STD_MODEL_INFO from  A_MODEL_20190617 t where t.MODEL_CATE_NAME='{}' '''.format(senti_words[k])
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
                            cn = list(self.segmentor.segment(
                                content.replace('\r', '').replace(' ', '').replace('\u3000', '')))
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
            if float(max_sim)>0.5:
                result=id2content[sim2id[max_sim]]
                return result
            else:
                return '您提供的信息不完整！抱歉'
        except:
            pass


    def doc_score(self, content):
        sents = self.seg_sentences(content)
        result_content = self.extract_most_sim_content(sents)
        return result_content



class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)


# Create server
server=ThreadXMLRPCServer(("10.9.1.199", 8896),requestHandler=RequestHandler)
server.register_introspection_functions()
# server.register_function(doc_sim_, 'doc_sim_')
# server.register_function(wb_sim_, 'wb_sim_')
# server.register_function(hash_sim_, 'hash_sim_')
server.register_instance(Doc_most_sim())
print("server is start...........")
# Run the server's main loop
server.serve_forever()
print("server is end...........")