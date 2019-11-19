#!/usr/bin/env python 
# -*- coding:utf-8 -*-
import tensorflow as tf
# from predict import RnnModel
from flask import jsonify
from flask import Flask
from flask import request
from sim_doc2vec import Sim_doc2vec
# from hash_sim import Hash_sim
import json

app = Flask(__name__)
model=Sim_doc2vec()
@app.route('/', methods=['POST','GET'])
def get_text_input():
    #return "connect successfully"
    #logging.info("connect successfully")
    tf_config = tf.ConfigProto()
    tf_config.gpu_options.allow_growth = True
    #http://127.0.0.1:5002/?inputStr="建设三路闸机坏了"
    text=request.args.get('inputStr')


    if text:
        aa=model.result_sim(text)
        return jsonify(aa)

if __name__ == "__main__":
    app.config['JSON_AS_ASCII'] = False
    app.run(host='127.0.0.1',port=5002)