#!/usr/bin/python
# -*- coding: UTF-8 -*-
import os
import secrets
from pydoc import cli
from re import M
import socket# 客户端 发送一个数据，再接收一个数据
import time
import zlib
from anyio import sleep
import rsa
from Crypto.Cipher import AES
from binascii import b2a_hex, a2b_hex
import pandas as pd
from tqdm import tqdm
import numpy as np
import struct
from ore import ore as ORE
import base64
import numpy as np
import random
# key = '6A4B3C7D9E2F1F3F'
# iv = 'qqqqqqqqqqqqqqqq'
key = secrets.token_bytes(16)
iv = secrets.token_bytes(16)
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
    # print("before add:", len(text))
    text = add_to_16(text)
    # print("after add:", len(text))
    cryptos = AES.new(key, mode, iv)
    cipher_text = cryptos.encrypt(text)
    # print("cipher len:", len(cipher_text))
    # 因为AES加密后的字符串不一定是ascii字符集的，输出保存可能存在问题，所以这里转为16进制字符串
    # return b2a_hex(cipher_text)
    # return base64.encodebytes(cipher_text)
    return cipher_text

# 解密后，去掉补足的空格用strip() 去掉
def decrypt(text) -> str:
    mode = AES.MODE_CBC
    cryptos = AES.new(key, mode, iv)
    # plain_text = cryptos.decrypt(a2b_hex(text))
    # plain_text = cryptos.decrypt(base64.decodebytes(text))
    plain_text = cryptos.decrypt(text)
    return bytes.decode(plain_text).rstrip('\0')

class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class TypeError(Error):
    '''
    输入数据类型不符合要求
    '''
    def __init__(self, message) -> None:
        self.name = 'TypeError'
        self.message = 'The datatype that this function get is not required, need bytes'

        

class monitor(object):
    '''
    数据载入压缩程序
    Input: filepath
    Output: compressed data list
    '''
    def __init__(self, filepath):
        # DATASET_ROOT_DIR = 'E://code//dataset_2'
        DATASET_ROOT_DIR = filepath
        self.file_list = list()

        # file_list = [f for f in os.listdir(DATASET_ROOT_DIR) if os.path.isfile(os.path.join(DATASET_ROOT_DIR, f))]
        self.file_list = [os.path.join(path, name) for path, subdirs, files in os.walk(DATASET_ROOT_DIR) for name in files]

        print(self.file_list)
        if len(self.file_list) > 1:
            dl = []
            for file in self.file_list:
                dl.append(pd.read_csv(file, low_memory=False).fillna(np.int64(0xffffff)))
            
            self.Readframe = pd.concat(dl, ignore_index=True)

        elif len(self.file_list) == 1:
            self.Readframe = pd.read_csv(self.file_list[0], low_memory=False).fillna(np.int64(0xffffff))

        # self.Readframe = pd.read_csv(filepath).fillna(np.int64(0xffffff))
        self.rownum = self.Readframe.shape[0]
        self.colnum = self.Readframe.shape[1]

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
                    total_str += str(word)
                total_str += '\n'
        # print(total_str)
        return total_str.rstrip('\n')
    
    def get_avg_record_length(self):
        total_length = 0
        for file in self.file_list:
            total_length += os.path.getsize(file)
        # for i in tqdm(range(self.rownum)):
        #     record = self.row2str(i)
        #     total_length += len(record.encode('utf-8'))
        print(total_length / self.rownum)



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
        data_store_length = []
        total_length = 0
        for i in tqdm(range(self.rownum)):
            # print(type(self.row2str(i)))
            record = self.row2str(i)
            if i % packsize == 0 and i != 0:
                
                row_data += bytes(record, encoding='utf-8')
                compress_data = zlib.compress(row_data)
                data_store.append(b2a_hex(compress_data))
                data_store_length.append(len(data_store[-1]))
                compress_rate += len(compress_data) / len(row_data)
                row_data = b''
            elif i == self.rownum - 1:
                row_data += bytes(record, encoding='utf-8')
                compress_data = zlib.compress(row_data)

                data_store.append(b2a_hex(compress_data))
                data_store_length.append(len(data_store[-1]))
                # compress_rate += len(compress_data) / len(row_data)
                row_data = b''
            else:
                row_data += bytes(record + "\t", encoding='utf-8')

        print(compress_rate / self.rownum)
        return compress_rate / self.rownum, data_store, data_store_length


class Store_system_client(object):

    def __init__(self, port, filepath, packsize, security=True, if_test=True) -> None:
        self.port = port
        self.host = socket.gethostname()
        self.filepath = filepath
        self.packsize = packsize
        self.ORE = ORE.ore()
        self.OREKey = self.ORE.rnd_word(10)
        self.if_test = if_test
        print("获取数据并压缩...")

        self.monitor = monitor(self.filepath)
        self.info_length = self.monitor.rownum
        rate , self.compress_data, self.data_origin_length =  self.monitor.compress_strbyrow_muti(self.packsize)
        self.packnums = len(self.compress_data)
        print(rate)


        self.BUFF_info = 0
        self.put_threshold = 10#插入记录缓存条数
        #插入操作状态码
        self.UPDATE_BUFF = -1
        self.UPDATE_BUFF_AND_CREATE_NEW_PACK = 0
        self.CREATE_NEW_PACK_ONLY = 1

        #删除操作状态码
        self.UPDATE_PACK = 501
        self.UPDATE_EMPTY_PACK = 502

        #查询操作状态码
        self.GET_INFO = 401
        self.GET_EMPTY_PACK = 402

        #终止操作状态码
        self.STOP_A = 404

        print("加密数据...")
        self.data = list()
        # self.data_origin_length = list()
        # for i in range(len(self.compress_data)):
            # self.compress_data[i] = b2a_hex(self.compress_data[i])
            # self.data_origin_length.append(len(self.compress_data[i]))
        start_time = time.time()
        self.data_origin_length.sort()
        if security:
            print("填充压缩包...")
            A = self.Gen_l_length()
            print(A)
            if A != -1:
                self.compress_data = self.add_pack(self.compress_data, A)
                print("完成填充。")
        


        for i in range(len(self.compress_data)):
            # print("before enc:",self.data_origin_length[i])
            self.data.append(encrypt(self.compress_data[i].decode()))
            # print("after enc:", len(self.data[i]))
        
        print("加密数据完成。")
        print("buildindex耗时:%f", time.time() - start_time)


        self.client = socket.socket(socket.AF_INET,socket.SOCK_STREAM) #声明socket类型，同时生成链接对象
        # self.client.connect(('47.101.183.28',12345)) #建立一个链接
        self.client.connect((socket.gethostname(), 12345))

        print("开始传输数据...")
        startmsg = b'datastream_start'
        self.pack_trans(startmsg)
        print("发送压缩包大小...")
        self.client.send(struct.pack('i', self.packsize))
        print(struct.unpack('i', struct.pack('i', len(self.data)))[0])
        # sleep(1)
        print("通知总数据包数量...")
        # self.packnums = len(self.data)
        seed_ = os.urandom(16)
        random.seed(seed_)
        random.shuffle(self.data)
        self.client.send(struct.pack('i', self.packnums))
        for i in tqdm(range(len(self.data))):
            self.pack_trans(self.ORE.ore_enc(bin(i*self.packsize)[2:], self.OREKey).encode('utf-8'))
            self.pack_trans(self.data[i])
        print("传输数据完成。")
        self.data = None
    
    def add_pack(self, pack_datas:list , A:int):
        for i in range(len(pack_datas)):
            if len(pack_datas[i]) % A:
                add = A - (len(pack_datas[i]) % A)
            else:
                add = 0
            pack_datas[i] = (pack_datas[i].decode() + ('\0' * add)).encode('utf-8')
        return pack_datas      



    def consist_MaxMinNormalization(self, x):
        x = x / self.max_consistency
        return x

    def std_MaxMinNormalization(self, x):
        x = x / self.max_std
        return x

    def cal_cost(self, L_length:list, fq_write=5e5, fq_get=10e4):
        avg_packsize = np.mean(L_length)

        storage_cost = self.packnums * avg_packsize

        # bandwidth_cost = (1 + fq_write)*avg_packsize*self.packsize
        bandwidth_cost = 2*fq_write*avg_packsize + fq_get*avg_packsize

        length_set = set(L_length)

        consistency = 0

        for item in length_set:
            consistency += self.packnums - L_length.count(item)
        
        security_risk = self.consist_MaxMinNormalization(consistency) + self.std_MaxMinNormalization(np.std(L_length, ddof=1))

        return storage_cost, bandwidth_cost, security_risk

    def cal_param(self, x, max, origin):
        return (x - origin) / (max - origin)
    
    def Gen_l_length(self, miu=0.1, seta=0.5, beta=0.1):
        '''
        miu:存储开销参数
        seta:带宽开销参数
        beta:安全开销参数
        '''
        max_length = self.data_origin_length[-1]
        max_length += 16 - (max_length % 16)

        length_set = set(self.data_origin_length)

        self.max_consistency = 0

        for item in length_set:
            self.max_consistency += self.packnums - self.data_origin_length.count(item)
        
        self.max_std = np.std(self.data_origin_length, ddof=1)

        origin_storage_cost = sum(self.data_origin_length)

        MAX_storage_cost = max_length * self.packnums

        fq_write = 5e5
        fq_get = 10e4

        origin_bandwidth_cost = (2*fq_write + fq_get)*(origin_storage_cost / self.packnums)

        MAX_bandwidth_cost = (2*fq_write + fq_get)*max_length
        
        print(max_length)
        MAX_L = int(max_length / 16)
        for i in reversed(range(1, MAX_L)):
            A = 16 * i
            L_length = list()
            for j in range(self.packnums):
                L_length.append(self.data_origin_length[j] + A - (self.data_origin_length[j] % A))
            storage_cost, bandwidth_cost, security_risk = self.cal_cost(L_length)
            if self.cal_param(storage_cost, MAX_storage_cost, origin_storage_cost) <= miu and self.cal_param(bandwidth_cost, MAX_bandwidth_cost, origin_bandwidth_cost) <= seta and security_risk / (self.max_consistency + self.max_std) <= beta:
                return A
        
        return -1
    
    def pack_trans(self, packdata) -> None:
        try:
            if not isinstance(packdata, bytes):
                raise TypeError(str(type(packdata)))
        except TypeError as e:
            print(e.name + ':' + e.message)

        data_length = len(packdata)
        # print(data_length)
        self.client.send(struct.pack('i', data_length))

        send_length = 0
        if data_length <= 1024:
            self.client.send(packdata)
        else:
            for j in range(int(data_length/1024)):
                self.client.send(packdata[j*1024:j*1024 + 1024])
                # send_length += len(packdata[j*1024:j*1024 + 1024])
            if data_length % 1024 != 0:
                self.client.send(packdata[int(data_length / 1024) * 1024 : ])
                # send_length += len(packdata[int(data_length / 1024) * 1024 : ])
        # print("sen:", send_length)
    
    def recv_data(self) -> bytes:
        '''
        从服务端接收数据
        '''
        total_data = bytes()
        header = self.client.recv(4, socket.MSG_WAITALL)
        length = struct.unpack('i', header)[0]
        while True:
            if length > 1024:
                length -= 1024
                row_data = self.client.recv(1024, socket.MSG_WAITALL)
                total_data += row_data
            else:
                row_data = self.client.recv(length, socket.MSG_WAITALL)
                total_data += row_data
                break
        return total_data

    def get_content_of_pack(self, rowdata) -> list:
        '''
        解密压缩包并取出内容
        '''
        return zlib.decompress(a2b_hex(decrypt(rowdata))).decode().split('\t')
        # return zlib.decompress(base64.decodebytes)

    
    
    def get_info(self) -> None:
        '''
        查询请求
        '''
        id = int(input('Please Input the ID of the record you want to get:'))
        if id >= 0 and id < self.info_length:

            # self.client.send(self.ORE.ore_enc(bin(id)[2:], self.OREKey).encode('utf-8'))
            self.pack_trans(self.ORE.ore_enc(bin(id)[2:], self.OREKey).encode('utf-8'))
            status_code = struct.unpack('i', self.client.recv(4, socket.MSG_WAITALL))[0]
            if status_code == self.GET_INFO:
                infoid = id % self.packsize
                total_data = self.recv_data()
            
                # pack = zlib.decompress(a2b_hex(decrypt(total_data))).decode().split('\t')
                pack = self.get_content_of_pack(total_data)
                if infoid < len(pack) and infoid >= 0:
                    print("get info:", pack[infoid])
                else:
                    print("the info has been deleted.")
            elif status_code == self.GET_EMPTY_PACK:
                print("the info has been deleted.")

            
        else:
            print("invalid id!")
        
        return None
    def get_range_info(self, start_id, end_id):
        '''
        范围查询
        '''
        range_pack = list()
        # start_id = int(input('Please Input the start ID of the record you want to get:'))
        # end_id = int(input('Please Input the end ID of the record you want to get:'))
        start_time = time.time()
        if start_id >= 0 and start_id < self.info_length and end_id >= 0 and end_id < self.info_length and end_id > start_id:
            self.pack_trans(self.ORE.ore_enc(bin(start_id)[2:], self.OREKey).encode('utf-8'))
            self.pack_trans(self.ORE.ore_enc(bin(end_id)[2:], self.OREKey).encode('utf-8'))
            pack_num = struct.unpack('i', self.client.recv(4, socket.MSG_WAITALL))[0]
            for i in range(pack_num):
                status_code = struct.unpack('i', self.client.recv(4, socket.MSG_WAITALL))[0]
                if status_code == self.GET_INFO:
                    range_pack.append(self.recv_data())
                    pack_content = self.get_content_of_pack(range_pack[-1])
                elif status_code == self.GET_EMPTY_PACK:
                    pass
        return time.time() - start_time
    
    def get_info_TEST(self, id) -> None:
        '''
        查询请求
        '''
        if id >= 0 and id < self.info_length:
            self.pack_trans(self.ORE.ore_enc(bin(id)[2:], self.OREKey).encode('utf-8'))
            status_code = struct.unpack('i', self.client.recv(4, socket.MSG_WAITALL))[0]
            if status_code == self.GET_INFO:
                infoid = id % self.packsize
                total_data = self.recv_data()

                pack = self.get_content_of_pack(total_data)

                if infoid < len(pack) and infoid >= 0:
                    pass
                else:
                    print("the info has been deleted")

            elif status_code == self.GET_EMPTY_PACK:
                print("the info has been deleted.")



        else:
            print("invalid id!")
        
        return None
    
    def pack2row(self, pack) -> bytes:
        '''
        解压后的压缩包重新打包封装
        '''
        new_row = ''
        for line in pack:
            new_row += line + '\t'
        return new_row.rstrip('\t').encode("utf-8")


    
    def put_info(self, new_content) -> None:
        '''
        插入记录
        '''

        # last_pack = zlib.decompress(a2b_hex(decrypt(self.data[-1]))).decode().split('\t')
        #从CSP下载包
        last_pack = self.get_content_of_pack(self.recv_data())

        if len(last_pack) < self.packsize:#默认插入最后一个包
            last_pack.append(new_content)
            # print(last_pack)
            new_data = self.pack2row(last_pack)
            # self.data[-1] = encrypt(zlib.compress(new_data))
            last_pack_data = encrypt(b2a_hex(zlib.compress(new_data)).decode())
            self.BUFF_info += 1
            if self.BUFF_info >= self.put_threshold:
                self.client.send(struct.pack('i', self.UPDATE_BUFF))
                self.pack_trans(last_pack_data)
                self.BUFF_info = 0
            else:
                self.client.send(struct.pack("i", self.STOP_A))
            return None
        else:
            self.packnums += 1
            if self.BUFF_info > 0:
                self.client.send(struct.pack('i', self.UPDATE_BUFF_AND_CREATE_NEW_PACK))
                self.pack_trans(last_pack_data)
            else:
                self.client.send(struct.pack('i', self.CREATE_NEW_PACK_ONLY))
            
            #更新CSP端的PID2PACK表
            self.pack_trans(self.ORE.ore_enc(bin(self.packnums)[2:], self.OREKey).encode('utf-8'))
            # self.data.append(encrypt(b2a_hex(zlib.compress(new_content.encode('utf-8'))).decode()))
            # self.data.append(encrypt(b2a_hex(self.compress_data[i]).decode()))
            
            #传送新包
            self.pack_trans(encrypt(b2a_hex(zlib.compress(new_content.encode('utf-8'))).decode()))
        
        return None

    def del_info(self, id) -> None:
        '''
        删除记录
        '''
        if id >= 0 and id < self.info_length:
            packid = int(id / self.packsize)
            pack = self.get_content_of_pack(self.data[packid])
            infoid = id % self.packsize
            del pack[infoid]
            if not pack:
                new_data = self.pack2row(pack)
                self.data[packid] = encrypt(b2a_hex(zlib.compress(new_data)).decode())
                self.client.send(struct.pack('i', self.UPDATE_PACK))
                self.client.send(struct.pack('i', packid))
                self.pack_trans(self.data[packid])
            else:
                self.data[packid] = bytes()
                self.client.send(struct.pack('i', self.UPDATE_EMPTY_PACK))
                self.client.send(struct.pack('i', packid))
        else:
            print("invalid id!")
        
        return None


    def TEST_GET(self) -> None:
        '''
        查询吞吐量
        '''
        total_time = 0
        for i in range(1000):
            start_time = time.time()
            self.pack_trans("get".encode('utf-8'))
            self.get_info_TEST(1)
            total_time += time.time() - start_time
        
        print("avg get time cost:", total_time / 1000)


                
    def activate_client(self):
        while True:
            # addr = client.accept()
            # print '连接地址：', addr

            try:
                if self.if_test:
                    self.TEST_GET()
                msg = input('Please Input Command:')  #strip默认取出字符串的头尾空格
                if msg == 'get':
                    # self.TEST_GET()
                    self.pack_trans(msg.encode('utf-8'))
                    self.get_info()

                elif msg == 'end':
                    self.pack_trans(msg.encode('utf-8'))
                    break
                elif msg == 'put':
                    # print("test once")
                    # self.pack_trans(msg.encode('utf-8'))
                    colnum = 13
                    new_content = ''
                    # new_record = ['ss','aa','bb','cc','dd','ee','ff','dd','aa','aa','bb','cc']
                    for i in range(colnum):
                        new_content += 'aaccbb\n'
                    
                    # self.put_info(new_content)
                    print("muti tests")
                    start_time = time.time()
                    for i in range(1000):
                        self.pack_trans(msg.encode('utf-8'))
                        self.put_info(new_content)
                    # self.put_info(new_content)
                    print("avg time cost:", (time.time() - start_time) / 10)
                elif msg == 'del':
                    self.pack_trans(msg.encode('utf-8'))
                    id = int(input("Please input id you want to del:"))

                    self.del_info(id)
                elif msg == 'range_get':
                    if self.if_test:
                        total_time = 0
                        for i in range(1000):
                            
                            self.pack_trans(msg.encode('utf-8'))
                            # start_id = int(input('Please Input the start ID of the record you want to get:'))
                            # end_id = int(input('Please Input the end ID of the record you want to get:'))
                            start_id = 0
                            end_id = start_id + (i + 1) * 9
                            total_time += self.get_range_info(start_id, end_id)
                        print("time cost:%f", total_time / 1000)
                    else:
                        self.pack_trans(msg.encode('utf-8'))
                        start_id = int(input('Please Input the start ID of the record you want to get:'))
                        end_id = int(input('Please Input the end ID of the record you want to get:'))
                        _ = self.get_range_info(start_id, end_id)

                else:
                    print("this command is not supported on this device")
            except ConnectionResetError as e:
                print('Connect lost')
                break
        self.client.close() #关闭这个链接

if __name__ == '__main__':
    # mon = monitor('E:\\code\\dataset_0')
    # mon.get_avg_record_length()

    # string = mon.row2str(0)
    # print(string)

    Client = Store_system_client(port=12345, filepath='./dataset_0', packsize=30)
    Client.activate_client()
    
    
            






        