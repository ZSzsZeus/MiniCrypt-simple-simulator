#!/usr/bin/python
# -*- coding: UTF-8 -*-

from pydoc import cli
import socket# 客户端 发送一个数据，再接收一个数据
import time
import zlib
import rsa
from Crypto.Cipher import AES
from binascii import b2a_hex, a2b_hex
import pandas as pd
from tqdm import tqdm
import numpy as np



class monitor(object):
    '''
    数据载入压缩程序
    Input: filepath
    Output: compressed data list
    '''
    def __init__(self, filepath):
        self.Readframe = pd.read_csv(filepath).fillna(np.int64(0xffffff))
        self.rownum = self.Readframe.shape[0]
        self.colnum = 13

    def row2str(self, id_row):
        # print(self.Readframe.loc[0].values[10].__class__)
        # print(self.Readframe.shape)
        total_str = str()
        for word in self.Readframe.loc[id_row].values:
            # print(word, word.__class__)
            if word != 0xffffff:
                if isinstance(word, str):
                    total_str += word
                else:
                    total_str += float(word).hex()
        # print(total_str)
        return total_str

    def compress_strbyrow(self):
        compress_rate = float()
        for i in range(self.rownum):
            row_data = bytes(self.row2str(i), encoding='utf-8')
            compress_data = zlib.compress(row_data)
            compress_rate += len(compress_data) / len(row_data)
        print(compress_rate / self.rownum)
        return compress_rate / self.rownum
    
    def compress_strbyrow_muti(self, packsize):
        compress_rate = float()
        row_data = b''
        data_store = []
        for i in tqdm(range(self.rownum)):
            # print(type(self.row2str(i)))
            record = self.row2str(i)
            if i % packsize == 0 and i != 0:
                
                row_data += bytes(record, encoding='utf-8')
                compress_data = zlib.compress(row_data)
                data_store.append(compress_data)
                compress_rate += len(compress_data) / len(row_data)
                row_data = b''
            elif i == self.rownum - 1:
                row_data += bytes(record, encoding='utf-8')
                compress_data = zlib.compress(row_data)
                data_store.append(compress_data)
                # compress_rate += len(compress_data) / len(row_data)
                row_data = b''
            else:
                row_data += bytes(record + "\t", encoding='utf-8')

        print(compress_rate / self.rownum)
        return compress_rate / self.rownum, data_store


client = socket.socket(socket.AF_INET,socket.SOCK_STREAM) #声明socket类型，同时生成链接对象
client.connect((socket.gethostname(),12345)) #建立一个链接，连接到本地的6969端口
BUFF = 1024
key = '6A4B3C7D9E2F1F3F'
iv = 'qqqqqqqqqqqqqqqq'

# try:
#     client.send(b'datastream_start')
#     mon = monitor

# 如果text不足16位的倍数就用空格补足为16位
def add_to_16(text) -> bytes:
    if len(text.encode('utf-8')) % 16:
        add = 16 - (len(text.encode('utf-8')) % 16)
    else:
        add = 0
    text = text + ('\0' * add)
    return text.encode('utf-8')


# 加密函数
def encrypt(text) -> bytes:
    mode = AES.MODE_CBC
    text = add_to_16(text)
    cryptos = AES.new(key.encode('utf-8'), mode, iv.encode('utf-8'))
    cipher_text = cryptos.encrypt(text)
    # 因为AES加密后的字符串不一定是ascii字符集的，输出保存可能存在问题，所以这里转为16进制字符串
    return b2a_hex(cipher_text)

# 解密后，去掉补足的空格用strip() 去掉
def decrypt(text) -> str:
    mode = AES.MODE_CBC
    cryptos = AES.new(key.encode('utf-8'), mode, iv.encode('utf-8'))
    plain_text = cryptos.decrypt(a2b_hex(text))
    return bytes.decode(plain_text).rstrip('\0')

while True:
    # addr = client.accept()
    # print '连接地址：', addr
    try:
        msg = input('Please Input Command:')  #strip默认取出字符串的头尾空格
        if msg == 'get':
            client.send(encrypt(msg))
            id = input('Please Input the ID of the record you want to get:')
        
            client.send(encrypt(id))
            infoid = int(client.recv(1024))
            total_data = bytes()
            while True:
                data = client.recv(1024)
                total_data += data
                if len(data) < 1024:
                    break
            pack = zlib.decompress(a2b_hex(decrypt(total_data))).decode().split('\t')
            print("recv:", pack[infoid])

            # print('recv:', data)
        elif msg == 'end':
            client.send(encrypt(msg))
            break
        elif msg == 'put':
            client.send(encrypt(msg))
            colnum = int(decrypt(client.recv(1024)))
            new_record = ['ss','aa','bb','cc','dd','ee','ff','dd','aa','aa','bb','cc']
            for i in range(colnum):
                client.send(encrypt('aa'))
            wait_result = decrypt(client.recv(1024))
            print(wait_result)
        else:
            client.send(encrypt(msg))  #发送一条信息 python3 只接收btye流
            data = decrypt(client.recv(1024)) #接收一个信息，并指定接收的大小 为1024字节
            print('recv:',data) #输出我接收的信息
    except ConnectionResetError as e:
        print('Connect lost')
        break
client.close() #关闭这个链接