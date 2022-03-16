#!/usr/bin/python
# -*- coding: UTF-8 -*-
# 文件名：server.py
 
import socket               # 导入 socket 模块
 
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         # 创建 socket 对象
host = socket.gethostname() # 获取本地主机名
port = 12345                # 设置端口
s.bind((host, port))        # 绑定端口
 
s.listen(5)                 # 等待客户端连接
while True:
    c,addr = s.accept()     # 建立客户端连接
    print('连接地址：', addr)
    # print '连接地址：', addr
    # c.send('欢迎访问菜鸟教程！')
    while True:
        try:
            data = c.recv(1024)
            #check
            if data != b'get' and data != b'put' and data != b'end':
                c.send(b'invalid command!')
            elif data == b'end':
                c.send(b'connect out')
                break
            else:
                print('recive:', data.decode())
                c.send(data.upper())
        except ConnectionResetError as e:
            print('connect lost')
            break
    
    c.close()                # 关闭连接
    break
