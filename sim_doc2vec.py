
import codecs
import re
import sys
import gensim
import sklearn
import numpy as np
import pandas as pd
import jieba
from gensim.models.doc2vec import Doc2Vec,LabeledSentence,TaggedDocument
from jieba import analyse
import jieba.posseg as pesg
import datetime
import os
import time
import json
import cx_Oracle

class Sim_doc2vec:
    def __init__(self):
        # python中对NLS_LANG实现设置
        os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
        cur_dir = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        self.path_qishu =os.path.join(cur_dir, 'data/train_w2v_10.txt')
        self.path_write =os.path.join(cur_dir, 'similarity_result/doc/' + 'sim_{}.txt'.format(time.time()))
        self.path_dict =os.path.join(cur_dir, 'data/index2id.json')


    def getData(self,user,password,database,targetTable,commandText):
        connection = cx_Oracle.connect(user,password,database)
        cursor = connection.cursor()
        cursor.execute(commandText.format(targetTable))
        x = cursor.description
        columns = [y[0] for y in x]
        cursor01 = cursor.fetchall()
        cursor.close()
        data = pd.DataFrame(cursor01,columns = columns)
        return data

    # def get_dataset(self):
    #     with open(self.path_qishu,'r',encoding='utf-8') as cf:
    #         docs=cf.readlines()
    #     x_train=[]
    #     for i,text in enumerate(docs):
    #         word_list=text.split(' ')
    #         l=len(word_list)
    #         word_list[l-1]=word_list[l-1].strip()
    #         document=TaggedDocument(word_list,tags=[i])
    #         x_train.append(document)
    #     return x_train

    # def train(self,x_train,size=300,epoch_num=1):
    #     model_dm=Doc2Vec(x_train,vector_size=256, window=10, min_count=5,\
    #                                   workers=4, alpha=0.025, min_alpha=0.025, epochs=12)
    #     model_dm.train(x_train,total_examples=model_dm.corpus_count,epochs=12)
    #     model_dm.save('model_doc/model_sim')
    #     return model_dm

    def ceshi(self,text_text):
        model_dm=Doc2Vec.load('model_doc/model_sim')
        inferred_vector_dm=model_dm.infer_vector(text_text.split(' '))
        sims=model_dm.docvecs.most_similar([inferred_vector_dm],topn=1)
        return sims

    def extract(self,line):
        #引用TF_IDF关键词抽取接口
        tfidf=analyse.extract_tags
        # str1_fenci=' '.join(jieba.cut(line))
        stop_word=[]
        with open('中文停用词库.txt','r',encoding='utf-8') as fp:
            for line in fp.readlines():
                line=line.strip()
                if line=='':
                    continue
                stop_word.append(line)
        str1_rv_stop_word=''
        str1_rv_stop_word_fenci=''
        for each in line.split(' '):
            if each not in stop_word:
                if str1_rv_stop_word=='':
                    str1_rv_stop_word=each
                    str1_rv_stop_word_fenci=each
                else:
                    str1_rv_stop_word=str1_rv_stop_word+each
                    str1_rv_stop_word_fenci=str1_rv_stop_word_fenci+' '+each

        guanjian=tfidf(str1_rv_stop_word_fenci)
        guanjian_result=''
        lianshi=[]
        for each in str1_rv_stop_word_fenci.split(' '):
            if each in guanjian:
                if guanjian_result=='':
                    guanjian_result=each
                else:
                    guanjian_result=guanjian_result+' '+each

                lianshi.append(each)

        return guanjian_result

    def result_sim(self,doc):

        ceshi_list = []
        # punc = './ <>_ - - = ", 。，？！“”：‘’@#￥% … &×（）——+【】{};；● &～| \s:'
        # stoplist = {}.fromkeys([line.rstrip() for line in
        #                         codecs.open(r"中文停用词库.txt", 'r', 'utf-8')])
        # # with open(path, 'r', encoding='utf-8') as f:
        # # list1 = f.read()
        # string = ''
        # X, Y = ['\u4e00', '\u9fa5']
        # text1 = re.sub(r'[^\w]+', '', doc)  # 将文本中的特殊字符去掉
        # # print(text1)
        # s = jieba.cut(text1)
        # s = [i for i in s if len(i) > 1 and X <= i <= Y and i not in stoplist]  # 匹配汉字且不在停用词内的
        # string = string.join(s)  # 合并分词
        # line1 = re.sub(r"[{}]+".format(punc), "", string)  # 将文本中标点符号去掉
        ceshi_list.append(doc)
        for line in ceshi_list:
            # f1.write(line + '\n' + '\n')
            line = self.extract(line)
            sims = self.ceshi(line)
            num = 0
            # df1 = pd.DataFrame()
            doc2sim_all = {}
            doc2sim_all_1 = []
            for count, sim in sims:
                doc2sim = {}
                num += 1
                with open('data/index2id.json') as f:
                    index2id = json.load(f)
                id = index2id[str(count)]
                user = 'dd_data'
                password = 'xdf123'
                database = 'LBORA170'
                targetTable = 'CS_JY_2'
                commandText = '''select t.STD_MODEL_INFO from CS_JY_2 t  where t.JY_ID='{}'
                                                    '''.format(str(id))
                # commandText ='''select t.STD_MODEL_INFO from CS_JY_2 t  where t.JY_ID='CCACPD0001' '''
                document = self.getData(user, password, database, targetTable, commandText)
                document = str(document['STD_MODEL_INFO'][0])
                sim = round(sim, 2)
                doc2sim['content'] = document
                doc2sim['sim_score'] = sim
                doc2sim['Fname'] = ''
                doc2sim_all_1.append(doc2sim)
            doc2sim_all['data'] = doc2sim_all_1
        return doc2sim_all

    def Comparison_two(self,fr1,fr2):
        punc = './ <>_ - - = ", 。，？！“”：‘’@#￥% … &×（）——+【】{};；● &～| \s:'
        stoplist = {}.fromkeys([line.rstrip() for line in
                                codecs.open(r"中文停用词库.txt", 'r', 'utf-8')])  # 建立字典，词库中的字符为键
        # with open(fr1, 'r', encoding='utf-8') as f:
        #     list1 = f.read()
        string = ''
        X, Y = ['\u4e00', '\u9fa5']
        text1 = re.sub(r'[^\w]+', '', fr1)  # 将文本中的特殊字符去掉
        # print(text1)
        s = jieba.cut(text1)
        s = [i for i in s if len(i) > 1 and X <= i <= Y and i not in stoplist]  # 匹配汉字且不在停用词内的
        string = string.join(s)  # 合并分词
        line1 = re.sub(r"[{}]+".format(punc), "", string)  # 将文本中标点符号去掉
        word_list1 = [word.word for word in pesg.cut(line1) if word.flag[0] not in ['w', 'x', 'u']]


        # with open(fr2, 'r', encoding='utf-8') as f:
        #     list2 = f.read()
        string = ''
        X, Y = ['\u4e00', '\u9fa5']
        text2 = re.sub(r'[^\w]+', '', fr2)
        # print(text2)
        s = jieba.cut(text2)
        s = [i for i in s if len(i) > 1 and X <= i <= Y and i not in stoplist]
        string = string.join(s)
        line2 = re.sub(r"[{}]+".format(punc), "", string)
        word_list2 = [word.word for word in pesg.cut(line2) if word.flag[0] not in ['w', 'x', 'u']]
        return word_list1, word_list2

    def sent2vec(self,model, words):
        """文本转换成向量

        Arguments:
            model {[type]} -- Doc2Vec 模型
            words {[type]} -- 分词后的文本

        Returns:
            [type] -- 向量数组
        """

        vect_list = []
        for w in words:
            try:
                vect_list.append(model.wv[w])
            except:
                continue
        vect_list = np.array(vect_list)
        vect = vect_list.sum(axis=0)
        return vect / np.sqrt((vect ** 2).sum())

    def similarity(self,vector1 , vector2):
        """计算两个向量余弦值

        Arguments:
            a_vect {[type]} -- a 向量
            b_vect {[type]} -- b 向量

        Returns:
            [type] -- [description]
        """
        cos1 = np.sum(vector1 * vector2)
        cos21 = np.sqrt(sum(vector1 ** 2))
        cos22 = np.sqrt(sum(vector2 ** 2))
        similarity = cos1 / float(cos21 * cos22)
        return similarity

    def test_model(self,fr1,fr2):
        print("load model")
        model = Doc2Vec.load('model_doc/model_sim')
        st1,st2 = self.Comparison_two(fr1,fr2)
        # 转成句子向量
        vect1 = self.sent2vec(model, st1)
        vect2 = self.sent2vec(model, st2)

        # 查看变量占用空间大小
        # import sys
        # print(sys.getsizeof(vect1))
        # print(sys.getsizeof(vect2))

        cos = self.similarity(vect1, vect2)
        # print("相似度：{:.4f}".format(cos))
        return cos

if __name__=='__main__':
    handle=Sim_doc2vec()
    # x_train =handle.get_dataset()
    with open(handle.path_dict) as f:
        index2id=json.load(f)
    flag = False
    if flag:
        fr1='txt3/5.txt'
        fr2='txt2/28.txt'
        two_sim=handle.test_model(fr1,fr2)
        print(two_sim)

    else:
        a = datetime.datetime.now()
        # path='txt4'
        # listdir=map(lambda x:os.path.join(path,x),os.listdir(path))
        # for i in listdir:
        i='轿车,手动档 进取型 国ⅢOBD 长城CC7130MM06轿车 长城 国产 精灵 200906.0'
        content=handle.result_sim(i)
        print(content)
        b = datetime.datetime.now()
        print("程序运行时间：" + str((b - a).seconds) + "秒")



