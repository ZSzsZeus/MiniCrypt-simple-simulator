#!/usr/bin/python
# -*- coding: UTF-8 -*-
# 文件名：store_system.py

# from audioop import add
# from email import header
from OpenSSL import crypto
from numpy.core.fromnumeric import compress
import pandas as pd
import numpy as np
import zlib
import matplotlib.pyplot as plt
import socket
from tqdm import tqdm
import des
import rsa
import time
from Crypto.Cipher import AES as aes
from binascii import b2a_hex, a2b_hex
import struct
from ore import ore as ORE

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
    
class Store_System(object):
    '''
    存储系统模拟程序,
    port = 端口号,
    filepath = 要加载的数据集的存储位置,
    packsize = 压缩数据集时每个压缩包中的记录条数
    '''
    def __init__(self, port) -> None:
        self.port = port
        self.host = socket.gethostname()
        self.packsize = None
        self.info_length = None
        self.ORE = ORE.ore()

        self.data = list()
        self.Pid2Pack = list()

        #插入操作状态码
        self.UPDATE_BUFF = -1
        self.UPDATE_BUFF_AND_CREATE_NEW_PACK = 0
        self.CREATE_NEW_PACK_ONLY = 1

        #删除操作状态码
        self.UPDATE_PACK_DEL = 501
        self.UPDATE_EMPTY_PACK_DEL = 502

        #查询操作状态码
        self.GET_INFO = 401
        self.GET_EMPTY_PACK = 402

        #中断操作状态码
        self.STOP_A = 404

        print("当前时间:"+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
        print("启动服务器")
        # AF_INET --> IPv4  SOCK_STREAM --> TCP 
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.bind((self.host, self.port))

        self.s.listen(5)


    def _Recvmessage(self) -> None:
        pass

    def _Sendmessage(self) -> None:
        pass

    def recv_large_data(self) -> bytes:
        total_data = bytes()
        head = self.c.recv(4, socket.MSG_WAITALL)
        length = struct.unpack('i', head)[0]
        # print("tes:", length)
        while True:
            if length > 1024:
                length -= 1024
                row_data = self.c.recv(1024, socket.MSG_WAITALL)
                total_data += row_data
            else:
                row_data = self.c.recv(length, socket.MSG_WAITALL)
                total_data += row_data
                break
        return total_data

    def send_pack(self, packdata) -> None:
        data_length = len(packdata)
        self.c.send(struct.pack('i', data_length))
        if data_length <= 1024:
            self.c.send(packdata)
        else:
            for j in range(int(data_length/1024)):
                self.c.send(packdata[j*1024:j*1024 + 1024])
            if data_length % 1024 != 0:
                self.c.send(packdata[int(data_length / 1024) * 1024 : ])




    def activate_server(self) -> None:
        # 服务器主程序
        while True:
            self.c, addr = self.s.accept()
            print("来自客户端{}的连接".format(addr))
            

            try:
                if not self.data:
                    print("接收数据中...")
                    #接收加密数据
                    self.msg = self.recv_large_data().decode()
                    print(self.msg)
                    if self.msg == 'datastream_start':
                        self.packsize = struct.unpack('i', self.c.recv(4, socket.MSG_WAITALL))[0]
                        print("压缩包长度:", self.packsize)
                        pack_nums = struct.unpack('i', self.c.recv(4, socket.MSG_WAITALL))[0]
                        print("数据包数量:", pack_nums)
                        for i in range(pack_nums):
                            # print(data_length)
                            # self.Pid2Pack.append(struct.unpack('i', self.c.recv(4, socket.MSG_WAITALL))[0])
                            self.Pid2Pack.append(self.recv_large_data().decode())
                            recv_data = self.recv_large_data()
                            self.data.append(recv_data)
                    print("接收数据完成。")
                    # print(self.Pid2Pack)
                    # print(len(self.data))
                    # print(len(self.data[0]))
                
                while True:
                    #接收客户端传输的命令
                    self.msg = self.recv_large_data().decode()
                    print(self.msg)
                    #判断命令合法性
                    if self.msg != 'get' and self.msg != 'put' and self.msg != 'end' and self.msg != 'del' and self.msg != 'range_get':                            
                        print("unexcept msg!")
                        
                    elif self.msg == 'end':
                        #中断连接
                        print("结束通信")
                        break
                    elif self.msg == 'get':#查询单一记录
                        id = self.recv_large_data().decode()
                        # print("要查询的id", id)
                        self.get_key(id)
                    elif self.msg == 'put':#插入记录
                        self.put_key()
                    elif self.msg == 'del':#删除记录
                        self.del_key()
                    elif self.msg == 'range_get':
                        startid = self.recv_large_data().decode()
                        endid = self.recv_large_data().decode()
                        self.get_range_key(startid, endid)

                
                break
                    
            except ConnectionResetError as e:
                print("connect lost")
                break
        
        self.c.close()
        return None
    def binarySearch(self, Get_ORE_Id:str, SearchList:list) -> int:
        '''
        二分查找PID
        Input:指定记录的加密ID,pack本地索引表
        Output:PID
        '''
        low = 0
        hi = len(SearchList) - 1
        mid = 0
        while(low <= hi):
            mid = int(low + (hi - low) / 2)
            # print(SearchList[mid])
            # print(Get_ORE_Id)
            if(self.ORE.ore_comp(SearchList[mid], Get_ORE_Id) == 0):
                return mid
            elif(self.ORE.ore_comp(SearchList[mid], Get_ORE_Id) == -1):
                low = mid + 1
            else:
                hi = mid - 1
        
        return mid


    def get_key(self, id:str) -> None:
        '''
        查询记录,
        id = 要查询的记录ID
        '''
        packid = self.binarySearch(id, self.Pid2Pack)
        print("查询的packid:",packid)
        if not self.data[packid]:
            self.c.send(struct.pack('i', self.GET_EMPTY_PACK))
        else:
            self.c.send(struct.pack('i', self.GET_INFO))
            self.send_pack(self.data[packid])
        return None
    
    def get_range_key(self, startid: str, endid: str) -> None:
        start_packid = self.binarySearch(startid, self.Pid2Pack)
        end_packid = self.binarySearch(endid, self.Pid2Pack)
        print("查询到的packid {} - {}".format(start_packid, end_packid))
        self.c.send(struct.pack('i', end_packid - start_packid + 1))

        for i in range(start_packid, end_packid + 1):
            if not self.data[i]:
                self.c.send(struct.pack('i', self.GET_EMPTY_PACK))
            else:
                self.c.send(struct.pack('i', self.GET_INFO))
                self.send_pack(self.data[i])
        
        return None

            
        


    def put_key(self) -> None:
        '''
        插入记录
        '''
        #向Client传输包
        self.send_pack(self.data[-1])
        status_code = struct.unpack('i', self.c.recv(4, socket.MSG_WAITALL))[0]
        # print(status_code)
        if status_code == self.UPDATE_BUFF:
            # print("UPDATA_BUFF")
            self.data[-1] = self.recv_large_data()
        elif status_code == self.UPDATE_BUFF_AND_CREATE_NEW_PACK:
            # print("UPDATE_BUFF_AND_CREATE_NEW_PACK")
            self.data[-1] = self.recv_large_data()
            self.Pid2Pack.append(self.recv_large_data().decode())
            self.data.append(self.recv_large_data())
        elif status_code == self.CREATE_NEW_PACK_ONLY:
            # print("CREATE_NEW_PACK_ONLY")
            self.Pid2Pack.append(self.recv_large_data().decode())
            self.data.append(self.recv_large_data())
        elif status_code == self.STOP_A:
            # print("STOP_A")
            return None
        # print("y")
        return None
    
    def del_key(self) -> None:
        '''
        删除记录
        '''
        status_code = struct.unpack('i', self.c.recv(4, socket.MSG_WAITALL))[0]

        if status_code == self.UPDATE_PACK_DEL:
            id = struct.unpack('i', self.c.recv(4, socket.MSG_WAITALL))[0]
            update_data = self.recv_large_data()
            self.data[id] = update_data
        if status_code == self.UPDATE_EMPTY_PACK_DEL:
            id = struct.unpack('i', self.c.recv(4, socket.MSG_WAITALL))[0]
            self.data[id] = bytes()
        else:
            print("statuscode error!")
        
        return None




if __name__ == '__main__':
    system = Store_System(port=12345)
    system.activate_server()



#test
# mon1 = monitor('E:\code\PRC Data Breach Chronology - 1.13.20.csv')



# packs = [1,10,20,30,50,100,200,300,400,500,600,700,800,1000,2000,3000]
# comrates = []

# for pack in packs:
#     comrates.append(mon1.compress_strbyrow_muti(pack))

# plt.plot(packs, comrates)
# plt.xlabel("packsize")
# plt.ylabel("compressratio")
# plt.title('Interesting Graph\nCheck it out') 
# plt.legend() 
# plt.show() 

# string = mon1.row2str(0)




# num = float(2.7)


# # string = str(num)

# print(num.hex().__class__)