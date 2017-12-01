#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
        datas['datetime'] = []
        datas['order_id'] = []
        datas['instrument'] = []
        datas['oc'] = []
        datas['bs'] = []
        datas['price'] = []
        datas['qty'] = []
        datas['remark'] = []
        order_sys_id_map = {}
        order_oc_map = {}
        for i in data:
            if u'OrderField' in i:
                datas['datetime'].append(datetime.fromtimestamp(i[u'OrderField']['TimeStamp'] / 1000.0 / 1000.0 / 1000.0))
                ctp_field = i[u'OrderField'][u'CThostFtdcOrderField']
                order_id = u'%s:%s:%s' % (ctp_field[u'FrontID'] ,  ctp_field[u'SessionID'] ,
                                          ctp_field[u'OrderRef'])
                if not order_sys_id_map.__contains__(order_id) and  len(ctp_field[u'OrderSysID']) != 0:
                    order_sys_id_map[ctp_field[u'OrderSysID']] = order_id
                    order_oc_map[ctp_field[u'OrderSysID']] = ctp_field[u'CombOffsetFlag']
                datas['instrument'].append(ctp_field[u'InstrumentID'])
                datas['order_id'].append(order_id)
                datas['oc'].append(ctp_field[u'CombOffsetFlag'])
                datas['bs'].append(ctp_field[u'Direction'])
                datas['price'].append(ctp_field[u'LimitPrice'])
                datas['qty'].append(ctp_field[u'VolumeTotal'])
                datas['remark'].append(ctp_field[u'StatusMsg'])
            elif u'TradeField' in i:
                datas['datetime'].append(datetime.fromtimestamp(i[u'TradeField']['TimeStamp'] / 1000.0 / 1000.0 / 1000.0))
                ctp_field = i[u'TradeField'][u'CThostFtdcTradeField']

                if order_sys_id_map.__contains__(ctp_field[u'OrderSysID']):
                    datas['order_id'].append(order_sys_id_map[ctp_field[u'OrderSysID']])
                else:
                    datas['order_id'].append('-1')
                datas['instrument'].append(ctp_field[u'InstrumentID'])
                datas['oc'].append(order_oc_map[ctp_field[u'OrderSysID']])
                datas['bs'].append(ctp_field[u'Direction'])
                datas['price'].append(ctp_field[u'Price'])
                datas['qty'].append(ctp_field[u'Volume'])
                datas['remark'].append(u'**成交**')
            else:
                print("Skip")
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
    df.set_index('datetime', inplace=True)
    df.sort_index(inplace=True)
    df.to_csv('result.csv', encoding='cp936')


if __name__ == '__main__':
    sys.exit(main())


