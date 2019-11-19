#!/usr/bin/env python 
# -*- coding:utf-8 -*-

def get_dict(input_path):
    # contents=[]
    public_numbers=[]
    car_serieses=[]
    for i,line in enumerate(open(input_path,'r',encoding='utf-8')):
        try:
            # content=line.strip().split('#')[0].split(' ')
            public_number=line.strip().split('#')[1]
            car_series=line.strip().split('#')[-1].strip()
            # contents.extend([i for i in content if len(i)>0 and len(i)<30])
            if public_number and 'nan' not in public_number:
                public_numbers.append(public_number)
            if car_series and 'nan' not in car_series:
                car_serieses.append(car_series)
        except:
            print(i,line)

    public_numbers=sorted(list(set(public_numbers)),key=lambda x:len(x),reverse=True)
    car_serieses=sorted(list(set(car_serieses)),key=lambda x:len(x),reverse=True)
    return public_numbers,car_serieses


if __name__=='__main__':
    input_path='data/standard.txt'
    public_numbers,car_serieses=get_dict(input_path)
    f1=open('data/dict.txt','w',encoding='utf-8')
    f2=open('data/car_series.txt','w',encoding='utf-8')
    f3=open('data/public_number.txt','w',encoding='utf-8')
    f1.write('\n'.join(public_numbers)+'\n'+'\n'.join(car_serieses))
    f2.write('\n'.join(car_serieses))
    f3.write('\n'.join(public_numbers))
    f1.close()
    f2.close()
    f3.close()