#!/usr/bin/env python 
# -*- coding:utf-8 -*-

import warnings

warnings.filterwarnings(action='ignore', category=UserWarning, module='gensim')
from gensim.models import word2vec
import gensim
import logging

# 主程序
# logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
# sentences = word2vec.Text8Corpus("data/train_data.txt")  # 加载语料
# n_dim = 200
# # 训练skip-gram模型;
# model = word2vec.Word2Vec(sentences, size=n_dim, min_count=10, sg=1)
# model.save('model1/model1.model')
model=gensim.models.word2vec.Word2Vec.load('model1/model1.model')
# 计算两个词的相似度/相关程度
y1 = model.n_similarity(u"手动档 进取型 国ⅢOBD 长城CC7130MM06轿车 长城 国产 精灵 200906.0", u"手动档 豪华型 国Ⅳ 长城CC7130MM02轿车 长城 国产 精灵 200802.0")
print(y1)
print("--------")
# 寻找对应关系
print(u"计算机-智能，速度-")
y3 = model.most_similar([u'智能', u'速度'], [u'计算机'], topn=3)
for item in y3:
    print(item[0], item[1])
