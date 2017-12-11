# !/usr/bin/env python
# encoding: utf-8


import sys
import os
import subprocess
import json
import pandas as pd
from datetime import datetime

def json_to_df(path):
    with open(path, 'r') as f:
        #data = json.loads(f.read().decode('cp936'))
        data = json.loads(f.read(),encoding='cp936')
        output = []
        datas = {}
        datas[u"direction" ]= []
        datas[u"position_effect_direction" ]= []
        datas[u"position_effect" ]= []
        datas[u"status" ]= []
        datas[u"qty" ]= []
        datas[u"leaves_qty" ]= []
        datas[u"trading_qty" ]= []
        datas[u"error_id" ]= []
        datas[u"raw_error_id" ]= []
        datas[u"input_price" ]= []
        datas[u"trading_price" ]= []
        datas[u"avg_price" ]= []
        datas[u"input_timestamp" ]= []
        datas[u"update_timestamp" ]= []
        datas[u"instrument_id" ]= []
        datas[u"exchange_id" ]= []
        datas[u"date" ]= []
        datas[u"order_id" ]= []
        datas[u"raw_error_message" ]= []
        for order in data:
            for key,value in order.items():
                datas[key].append(value)
    return pd.DataFrame(datas)

def main():
    for f in os.listdir(sys.argv[1] + '/'):
        if f.find('json') != -1:
            continue
        subprocess.call(['ctp_serialize_reader.exe', os.path.join(sys.argv[1], f)])
    for f in os.listdir(sys.argv[1] + '/'):
        if f.find('json') == -1:
            continue
        lines = []
# replace no assic char
        json_file_path = os.path.join(sys.argv[1], f)
        with open(json_file_path) as json_file:
            lines = json_file.readlines()
            json_file.seek(0, 0)
        with open(json_file_path, 'w') as json_file:
            json_file.writelines([x.replace('"\x00"', '""') for x in lines])

    dfs = []
    for f in os.listdir(os.sys.argv[1]):
        if f.find('json') == -1:
            continue
        df = json_to_df(os.path.join(os.sys.argv[1], f))
        df['account'] = f[:f.find('_')]
        dfs.append(df)

    df = pd.concat(dfs)
    #  df.set_index('datetime', inplace=True)
    #  df.sort_index(inplace=True)
    df.to_csv('order_field_result.csv')
    return 0

if __name__ == '__main__':
    sys.exit(main())
