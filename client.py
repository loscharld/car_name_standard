#!/usr/bin/env python
# -*- coding:utf-8 -*-
import xmlrpc.client
import datetime

server = xmlrpc.client.ServerProxy("http://10.9.1.199:8896")
start_time=datetime.datetime.now()
content1='传祺GAH7150H2A5轿车广汽传祺GS3广汽传祺广汽传祺GS3(17-)5广汽乘用车传祺GS3 150N 手自一体 精英版 国Ⅴ运动型多功能车传祺GAH7150H2A5轿车传祺GAH7150H2A5轿车广汽传祺GQBAXD0013'
content2='轿车类宝马BMW7200SL(BMW320Li)轿车华晨宝马3系宝马华晨宝马3系F35(18-)5华晨宝马华晨宝马320Li 手自一体 时尚型 长轴距版 国Ⅵ三厢轿车宝马BMW7200SL(BMW320Li)轿车宝马BMW7200SL(BMW320Li)轿车华晨宝马BMAAHD0167'
content3='东风DFH5160XXYEX3B厢式运输车东风天锦货车东风东风天锦KR3东风汽车发动机型号:福田康明斯ISD180 62;底盘型号:东风DFH1160E3载重货车东风DFH5160XXYEX3B厢式运输车东风DFH5160XXYEX3B厢式运输车东风'
content4='轿车类大众FV7160BBMBG轿车一汽大众捷达2015大众一汽大众捷达(13-17)5一汽大众捷达质惠版1.6L MT时尚型 国Ⅴ三厢轿车大众FV7160BBMBG轿车大众FV7160BBMBG轿车大众DZAATD0091'
content5='豪门HM48Q-6A两轮轻便摩托车摩托车摩托车摩托车车组（5千以下）1摩托车生产厂家摩托车豪门HM48Q-6A两轮轻便摩托车豪门HM48Q-6A两轮轻便摩托车摩托车'
list=[content1,content2,content4,content5]
for i in range(25):
    for i in list:
        result=server.doc_score(i)
        print(result)
end_time=datetime.datetime.now()
# print(result)
print('总耗时：%.1f秒' % ((end_time - start_time).seconds))