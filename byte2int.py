sting = b'1'

print(int(sting))

string = b'helloworld'

print(string.decode().split(' '))

s = 's'
print("\t" + s)
import struct
import zlib

pack = zlib.compress(string)

print(zlib.decompress(pack).decode().split(' '))

print(int(64/30))


from Crypto.Cipher import AES
from binascii import b2a_hex, a2b_hex


# 如果text不足16位的倍数就用空格补足为16位
def add_to_16(text):
    if len(text.encode('utf-8')) % 16:
        add = 16 - (len(text.encode('utf-8')) % 16)
    else:
        add = 0
    text = text + ('\0' * add)
    return text.encode('utf-8')


# 加密函数
def encrypt(text):
    key = '9999999999999999'.encode('utf-8')
    mode = AES.MODE_CBC
    iv = b'qqqqqqqqqqqqqqqq'
    text = add_to_16(text)
    cryptos = AES.new(key, mode, iv)
    cipher_text = cryptos.encrypt(text)
    # 因为AES加密后的字符串不一定是ascii字符集的，输出保存可能存在问题，所以这里转为16进制字符串
    return b2a_hex(cipher_text)


# 解密后，去掉补足的空格用strip() 去掉
def decrypt(text):
    key = '9999999999999999'.encode('utf-8')
    iv = b'qqqqqqqqqqqqqqqq'
    mode = AES.MODE_CBC
    cryptos = AES.new(key, mode, iv)
    plain_text = cryptos.decrypt(a2b_hex(text))
    return bytes.decode(plain_text).rstrip('\0')

class numd(object):
    def __init__(self) -> None:
        self.num = 10
    def add(self):
        self.num += 1
    def printnum(self):
        print(self.num)


if __name__ == '__main__':

    for i in range(1,1):
        print(i)
    bbb = bytes()
    if not bbb:
        print("y")
    e = encrypt("hello world")  # 加密
    d = decrypt(e)  # 解密
    print(type(d))
    print("加密:", e)
    print("解密:", d)

    key = None
    if not key:
        print("y")

    string3 = 'www.baidu.com'
    print(string3[0:2])
    for i in range(0, 4097, 1024):
        print(i)
    def add_to_1024(text) -> str:
        text = str(text)
        if len(text.encode('utf-8')) < 1024:
            add = 1024 - len(text.encode('utf-8')) % 1024
        else:
            add = 0
        
        text = text + (add * '\0')
        return text

    num = 28878
    num_ = add_to_1024(num)
    print(num_.encode('utf-8'))
    num__ = num_.encode('utf-8')
    print(num)

    print(int(num__.decode().rstrip('\0')))

    print((str(num.__class__)))
    print(type(str(num.__class__)))

    numds = numd()
    numds.printnum()
    numds.add()
    numds.printnum()
    minusnum = struct.pack('i', 23200)
    print(len(minusnum))
    print(struct.unpack('i', minusnum)[0])
    strings = 'www.baidu.com'
    for i in range(int(len(strings) / 2)):
        print(strings[i*2:i*2 + 2])
    print(strings[int(len(strings) / 2) * 2:])

    # GETtimelist = [,]



