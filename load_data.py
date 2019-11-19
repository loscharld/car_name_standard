#!/usr/bin/env python3
# coding: utf-8
# File: demo.py.py
# Author: lhy<lhy_in_blcu@126.com,https://huangyong.github.io>
# Date: 18-4-25

class DataLoader:
    def __init__(self):
        self.datafile = 'data1/train_data.txt'
        self.dataset = self.load_data()

    '''加载数据集'''
    def load_data(self):
        dataset = []
        for line in open(self.datafile,encoding='utf-8'):
            line = line.strip().split(' ')
            dataset.append([word for word in line if len(line) >6])
        return dataset
