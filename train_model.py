import pandas as pd
import re
import codecs
import jieba
import jieba.analyse
# import numpy as np
import datetime
import json
import gensim
# from gensim import corpora,models,similarities  #用于tf-idf
from gensim.models.doc2vec import Doc2Vec,LabeledSentence,TaggedDocument
# from gensim.models.word2vec import LineSentence, Word2Vec

# from collections import defaultdict   #用于创建一个空的字典，在后续统计词频可清理频率少的词语

#连数据库
import cx_Oracle
# from sqlalchemy import create_engine
# from sklearn.ensemble import IsolationForest
# import warnings
import os
from load_data import *

class Train_model:
    def __init__(self):
        os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
        cur_dir = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        self.stop_words_path=os.path.join(cur_dir, '中文停用词库.txt')
        self.dict_paths=os.path.join(cur_dir, 'data')
        self.dict_path=os.path.join(self.dict_paths, 'index2id.json')
        self.model_paths = os.path.join(cur_dir, 'model_doc')
        self.model_path=os.path.join(self.model_paths, 'model_sim')
        self.dataset = DataLoader().dataset




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

    def process_data(self,train_data):
        # punc = './ <>_ - - = ", 。，？！“”：‘’@#￥% … &×（）——+【】{};；● &～| \s:'
        stoplist = {}.fromkeys([line.rstrip() for line in
                                codecs.open(self.stop_words_path, 'r', encoding='utf-8')])  # 建立字典，词库中的字符为键

        a = datetime.datetime.now()
        count = 0
        sentences = []
        id2index = {}
        for i in range(len(train_data)):
            list1 = str(train_data['STD_MODEL_INFO'][i])
            id = str(train_data['JY_ID'][i])
            id2index[id] = len(id2index)
            # string = ' '
            # X, Y = ['\u4e00', '\u9fa5']
            # text1 = re.sub(r'[^\w]+', '', list1)  # 将文本中的特殊字符去掉
            # print(text1)
            # s = jieba.cut(text1)
            # s = [i for i in list1 if len(i) > 1 and X <= i <= Y and i not in stoplist]  # 匹配汉字且不在停用词内的
            # sentence = string.join(s)
            # sentence = re.sub(r"[{}]+".format(punc), "", s)  # 将文本中标点符号去掉
            sentences.append(list1)
            count += 1
            print(count)
        try:
            index2id = {v: str(k) for k, v in id2index.items()}
        except Exception as e:
            print(e)
        with open(self.dict_path, 'w') as f:
            json.dump(index2id, f)
        b = datetime.datetime.now()
        print("数据处理总用时：" + str((b - a).seconds) + "秒")
        # with open('data/train_w2v_11.txt', 'w', encoding='utf-8') as writer:
        #     writer.write('\n'.join(sentences))
        return sentences

    def read_corpus(self,documents):
        for i, plot in enumerate(documents):
            yield gensim.models.doc2vec.TaggedDocument(gensim.utils.simple_preprocess(plot, max_len=30), [i])

    def get_dataset(self,docs):
        x_train=[]
        for i,text in enumerate(docs):
            word_list=text.split(' ')
            l=len(word_list)
            word_list[l-1]=word_list[l-1].strip()
            document=TaggedDocument(word_list,tags=[i])
            x_train.append(document)
        return x_train


    def train(self,x_train,size=300,epoch_num=1):
        model_dm=Doc2Vec(x_train,vector_size=300, window=10, min_count=5,\
                                      workers=4, alpha=0.025, min_alpha=0.025, epochs=12)
        model_dm.train(x_train,total_examples=model_dm.corpus_count,epochs=12)
        model_dm.save(self.model_path)
        return model_dm

if __name__=='__main__':
    # user = 'dd_data'
    # password = 'xdf123'
    # database = 'LBORA170'
    # targetTable = 'soure_01'
    # commandText = '''select t.JY_ID,t.STD_MODEL_INFO from CS_JY_2 t
    #             '''
    start_time=datetime.datetime.now()
    handle=Train_model()
    if not os.path.exists(handle.model_paths):
        os.mkdir(handle.model_paths)
    if not os.path.exists(handle.dict_paths):
        os.mkdir(handle.dict_paths)
    # train_data = handle.getData(user, password, database, targetTable, commandText)
    # sentences=handle.process_data(train_data)
    sentences = handle.dataset
    x_train=list(handle.read_corpus(sentences))
    print('开始训练模型。。。。。。')
    model_dm = handle.train(x_train)
    end_time = datetime.datetime.now()
    print('训练模型总耗时：%.1f秒' % ((end_time - start_time).seconds))