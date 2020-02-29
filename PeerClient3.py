import socket
import os
import unicodedata
import hashlib
from multiprocessing import Process
from multiprocessing import Pool
import prettytable as pt
import json
from time import ctime, sleep
import multiprocessing as mp
import threading
import base64
import struct
import sys
import datetime
import time
import cv2
import numpy as np
import math

######################################################################
############################## 全局变量 ##############################
######################################################################

filePath = "/Users/elmagnifico/Desktop/python/com/Task2_P2P/Peer/Peer3Data/"  # 文件存储路径
BUFFSIZE = 2048  # 文件传输缓存
PEER_IP = "127.0.0.1"  # 节点IP地址
PEER_PORT = 8005  # 节点端口
PEER_DOWNLOAD_IP = "127.0.0.1"  # 用于其他节点下载的IP地址
PEER_DOWNLOAD_PORT = 8006  # 用于其他节点下载的端口
SERVER_IP = "127.0.0.1"  # 服务器IP地址
SERVER_PORT = 8080  # 服务器端口
FILEandMD5 = {}
# 传送包的结构定义
# 包括 序列号，确认号，文件结束标志，1024B的数据
pkt_struct = struct.Struct('III1024s')
# 信息反馈包
# 包括 ack rwnd
fb_struct = struct.Struct('II')
FILE_SIZE = 1024  # 读取文件的大小
streamEndFlag = 0  # 流结束标志


############################################################################
############################# MD5计算 ######################################
############################################################################

############################ 计算字符串MD5 #####################################
def calMD5(str):
    # 获取一个md5加密算法对象
    m = hashlib.md5()
    # 制定需要加密的字符串
    m.update(str)
    # 获取加密后的16进制字符串
    return m.hexdigest()


############################ 计算文件MD5 #####################################
def calMD5ForFile(file):
    stat_info = os.stat(filePath + file)
    if int(stat_info.st_size) / (1024 * 1024) >= 1000:  # 大文件
        return calMD5ForBigFile(filePath + file)
    m = hashlib.md5()
    f = open(filePath + file, 'rb')
    m.update(f.read())
    f.close()
    return m.hexdigest()


############################ 计算大文件MD5 #####################################
def calMD5ForBigFile(file):
    m = hashlib.md5()
    f = open(file, 'rb')
    buffer = 8192
    while 1:
        chunk = f.read(buffer)
        if not chunk:
            break
        m.update(chunk)
    f.close()
    return m.hexdigest()


############################ 计算文件大小 #####################################
def get_FileSize(filePath):
    fsize = os.path.getsize(filePath)
    fsize = fsize / float(1024 * 1024)
    return round(fsize, 2)


############################ 获取文件列表 #####################################
def getFileList():
    # 得到文件资源列表，类型为list，转为 资源1$资源2
    # 格式为 [ {jsondata1}, {jsondata2} ...]
    file_dir = filePath
    dataFileList = []  # 文件名+文件大小
    fileSet = []
    for root, dirs, files in os.walk(file_dir):
        # root 当前目录路径
        # dirs 当前路径下所有子目录
        # files 当前路径下所有非目录子文件
        fileSet = files

    for f in fileSet:
        fileSize = get_FileSize(file_dir + f)
        fileMD5 = calMD5ForFile(f)
        fileInfo = dict(fileName=f, fileSize=fileSize, fileMD5=fileMD5)
        jsonData = json.dumps(fileInfo)
        dataFileList.append(jsonData)

    return dataFileList


########################################################################
############################## 通信指令 #################################
########################################################################

REGISTER = "register"  # 请求服务器注册peer
REQUEST = "request"  # 请求服务器返回资源对应的peer列表
UPDATE = "update"  # 更新peer资源信息
DOWNLOAD = "download"  # 请求peer下载资源命令
BIGDOWNLOAD = "bigdownload"  # 下载大文件
UPLOAD = "upload"  # 上传资源给其他peer
EXIT = "exit"  # 退出客户端
STREAM = "stream"  # 发送视频流
BIGSTREAM = "bigstream"  # 发送视频流(无压缩)
STOP = "stop"  # 停止发送视频流
REGISTER_SUCESSFULLY = "节点注册成功"  # 注册成功
REGISTER_FAILED = "节点注册失败"  # 注册失败
HAVE_REGISTERED = "该节点已经注册，无需重复注册"  # 重复注册
UPDATE_SUCESSFULLY = "节点更新资源成功"  # 更新成功
UPDATE_FAILED = "节点更新资源失败"  # 更新失败
SEND_SUCESSFULLY = "发送成功"  # 发送成功
SEND_FAILED = "发送失败"  # 发送失败
DELETE_SUCCESSFULLY = "删除节点信息成功"  # 删除节点成功
DELETE_FAILED = "删除节点信息失败"  # 删除节点失败
DOWNLOAD_SUCESSFULLY = "下载成功"  # 下载成功
DOWNLOAD_FAILED = "下载失败"  # 下载失败
DOWNLOAD_PREPARE = "准备下载"  # 准备下载资源
SERVER_ERROR = "服务器错误"  # 服务器错误
FILE_NO_EXIST = "文件不存在"  # 文件不存在

#########################################################################
############################ 客户端SOCKET ################################
#########################################################################

peerClientAddress = (PEER_IP, PEER_PORT)
mainPeerClientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
mainPeerClientSocket.bind(peerClientAddress)

serverAddress = (SERVER_IP, SERVER_PORT)

peerClientDownloadAddress = (PEER_IP, PEER_DOWNLOAD_PORT)
DownloadSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
DownloadSocket.bind(peerClientDownloadAddress)


############################################################################
########################### 信息传输函数 ####################################
############################################################################

############################ 注册节点 #####################################
def registerOnServer():
    mainPeerClientSocket.sendto(REGISTER.encode('utf-8'), serverAddress)
    peerData = []
    peerData.append(str(PEER_DOWNLOAD_PORT))
    peerData += getFileList()  # 文件列表（文件名 文件大小 文件MD5）
    mainPeerClientSocket.sendto('$'.join(peerData).encode('utf-8'), serverAddress)  # 列表用 $ 分割

    try:
        data, addr = mainPeerClientSocket.recvfrom(BUFFSIZE)
        if data.decode('utf-8') == REGISTER_SUCESSFULLY:
            print(REGISTER_SUCESSFULLY)
        elif data.decode('utf-8') == REGISTER_FAILED:
            print(REGISTER_FAILED)
        elif data.decode('utf-8') == HAVE_REGISTERED:
            print(HAVE_REGISTERED)
        else:
            print("发生未知错误")
    except:
        print("超时未响应")
    return 0


############################ 请求资源 #####################################
def requestPeerSource(fileName):
    cmd = REQUEST + ' ' + fileName
    mainPeerClientSocket.sendto(cmd.encode('utf-8'), serverAddress)
    data, addr = mainPeerClientSocket.recvfrom(BUFFSIZE)
    msg = data.decode().split('$')
    if msg[0] == '':
        print("找不到该资源")
    else:
        print("资源列表如下：")
        tb = pt.PrettyTable()
        # 使用英文，不会出现对齐出错现象
        tb.field_names = ['IP Address', 'Download Port', 'Name', 'File Size(MB)', 'MD5', 'Latest Update']
        for i in range(0, len(msg)):
            data = msg[i].split(',')
            if len(data) < 6:
                continue
            list = []
            for k in range(0, len(data)):
                if k % 2 == 0 or k == 5:
                    name = ''
                    for j in range(2, len(data[k]) - 1):
                        if k == 5 and j == len(data[k]) - 2:
                            continue
                        name += data[k][j]
                    list.append(name)
                else:
                    name = ''
                    for j in range(1, len(data[k])):
                        name += data[k][j]
                    list.append(name)
            tb.add_row([list[0], list[1], list[2], list[3], list[4], list[5]])
        print(tb)
        for i in range(0, len(msg)):
            name = ''
            md5 = ''
            data = msg[i].split(',')
            if len(data) < 6:
                break
            for k in range(0, len(data)):
                if k == 2:
                    for j in range(2, len(data[k]) - 1):
                        name += data[k][j]
                if k == 4:
                    for j in range(2, len(data[k]) - 1):
                        md5 += data[k][j]
            FILEandMD5[name] = md5
        return 0


############################ 请求资源 #####################################
def requestAllSource():
    cmd = REQUEST
    mainPeerClientSocket.sendto(cmd.encode('utf-8'), serverAddress)
    data, addr = mainPeerClientSocket.recvfrom(BUFFSIZE * 10)
    msg = data.decode().split('$')
    if msg[0] == '':
        print("资源列表为空")
    else:
        print("资源列表如下：")
        tb = pt.PrettyTable()
        # 使用英文，不会出现对齐出错现象
        tb.field_names = ['IP Address', 'Download Port', 'Name', 'File Size(MB)', 'MD5', 'Latest Update']
        for i in range(0, len(msg)):
            data = msg[i].split(',')
            if len(data) < 6:
                continue
            list = []
            for k in range(0, len(data)):
                if k % 2 == 0 or k == 5:
                    name = ''
                    for j in range(2, len(data[k]) - 1):
                        if k == 5 and j == len(data[k]) - 2:
                            continue
                        name += data[k][j]
                    list.append(name)
                else:
                    name = ''
                    for j in range(1, len(data[k])):
                        name += data[k][j]
                    list.append(name)
            tb.add_row([list[0], list[1], list[2], list[3], list[4], list[5]])
        print(tb)
        for i in range(0, len(msg)):
            name = ''
            md5 = ''
            data = msg[i].split(',')
            if len(data) < 6:
                break
            for k in range(0, len(data)):
                if k == 2:
                    for j in range(2, len(data[k]) - 1):
                        name += data[k][j]
                if k == 4:
                    for j in range(2, len(data[k]) - 1):
                        md5 += data[k][j]
            FILEandMD5[name] = md5
        return 0


########################## 进度条打印 ###################################
def progress_bar(num_cur, total):
    ratio = float(num_cur) / total
    percentage = int(ratio * 100)
    r = '\r[%s%s]%d%%' % (">" * percentage, " " * (100 - percentage), percentage)
    sys.stdout.write(r)
    sys.stdout.flush()


########################## 下载其他节点资源 ###################################
def downloadSourceFromPeer(FILE, IP_LIST, PORT_LIST):
    # 单点传输
    if len(IP_LIST) == 1:
        goalAddr = []
        goalAddr.append(IP_LIST[0])
        goalAddr.append(PORT_LIST[0])
        goalAddressTuple = tuple(goalAddr)
        source = DOWNLOAD + " " + FILE + " " + "0"
        mainPeerClientSocket.sendto(source.encode(), goalAddressTuple)
        try:
            data, addr = mainPeerClientSocket.recvfrom(BUFFSIZE)
        except Exception as e:
            print(e)
        if data.decode() == FILE_NO_EXIST:
            print("文件不存在")
        else:
            full_length = int(data.decode())
            now_length = 0
            print("正在下载")
            start_time = time.clock()
            try:
                count = 0
                while True:
                    if count == 0:
                        data, client_addr = mainPeerClientSocket.recvfrom(1024)
                        f = open(filePath + 'Received-' + data.decode(), 'wb')
                    data, client_addr = mainPeerClientSocket.recvfrom(1024)
                    if str(data) != "b'end'":
                        now_length += len(data)
                        progress_bar(now_length, full_length)
                        f.write(data)
                    else:
                        break
                    count += 1
                end_time = time.clock()
                full_time = end_time - start_time
                speed = full_length / (full_time * 1024 * 1024)
                speed = round(speed, 2)
                print("\n下载完毕,下载速度为 " + str(speed) + " MB/s")
                f.close()
                current_fileMD5 = calMD5ForFile('Received-' + FILE)
                # print(current_fileMD5)
                # print(FILEandMD5)
                MD5IsIn = 0
                for k, v in FILEandMD5.items():
                    if k == FILE:
                        MD5IsIn = 1
                        break
                if MD5IsIn == 1:
                    if current_fileMD5 != FILEandMD5[FILE]:
                        print("经MD5检验，接收的文件不完整")
                    else:
                        print("经MD5检验，接收文件正确")
                else:
                    if now_length != full_length:
                        print("下载的文件不完整")
                    else:
                        print("下载的文件完整")
            except Exception as e:
                print(e)
                print("文件接收失败")

    # 双点传输
    elif len(IP_LIST) == 2:
        goalAddr1 = []
        goalAddr1.append(IP_LIST[0])
        goalAddr1.append(PORT_LIST[0])
        goalAddr1Tuple = tuple(goalAddr1)
        sourceBlock1_CMD = DOWNLOAD + " " + FILE + " " + "1"

        goalAddr2 = []
        goalAddr2.append(IP_LIST[1])
        goalAddr2.append(PORT_LIST[1])
        goalAddr2Tuple = tuple(goalAddr2)
        sourceBlock2_CMD = DOWNLOAD + " " + FILE + " " + "2"

        mainPeerClientSocket.sendto(sourceBlock1_CMD.encode('utf-8'), goalAddr1Tuple)
        mainPeerClientSocket.sendto(sourceBlock2_CMD.encode('utf-8'), goalAddr2Tuple)

        data, addr = mainPeerClientSocket.recvfrom(BUFFSIZE * 2)
        data1, addr1 = mainPeerClientSocket.recvfrom(BUFFSIZE * 2)

        if not data:
            print(DOWNLOAD_FAILED)
        elif data.decode('utf-8') == DOWNLOAD_PREPARE:
            # 接收两个分组的数据
            # 使用一个列表 dataList 来维护接收的无序的消息队列
            # 当找到有序的消息时就写入文件。
            dataQueue = []  # 该列表会接收暂时无法构成有序分组的消息
            requestNum = 0  # 该数字表示当前文件需要的分组序号
            download1_sucess_flag = False
            download2_sucess_flag = False
            try:
                with open(filePath + "Received-" + FILE, 'wb') as f:
                    while True:
                        # 传输完成，直接退出，否则下面recvfrom没有可以接收的数据
                        if download1_sucess_flag == True and download2_sucess_flag == True:
                            break

                        data, addr = mainPeerClientSocket.recvfrom(BUFFSIZE * 2)

                        # 第一个peer传输完成
                        if data.decode('utf-8') == DOWNLOAD_SUCESSFULLY + "-1":
                            download1_sucess_flag = True
                            continue
                        # 第二个peer传输完成
                        if data.decode('utf-8') == DOWNLOAD_SUCESSFULLY + "-2":
                            download2_sucess_flag = True
                            continue
                        # 传输完成，直接退出
                        if download1_sucess_flag == True and download2_sucess_flag == True:
                            break

                        if download1_sucess_flag == False or download2_sucess_flag == False:
                            # 将字符串转为dict(Num , fileData_base64)
                            raw_data = json.loads(data.decode('utf-8'))
                            # 分块的编号
                            packetNum = raw_data["Num"]
                            # base4字符串解码成文件字节码
                            file_block_data = base64.b64decode(raw_data["Data"])

                            # 当前消息是需要的序号分组，那么就会读入文件
                            if packetNum == requestNum:
                                f.write(file_block_data)
                                requestNum += 1
                            else:
                                # 当前消息不是需要的序号分组，那么会加入队列
                                packet = dict(Num=packetNum, Data=file_block_data)
                                dataQueue.append(packet)
                                # 查找队列中有无需要的序号
                                index = 0
                                while True:
                                    if index >= len(dataQueue):
                                        break
                                    if dataQueue[index]["Num"] == requestNum:
                                        f.write(dataQueue[index]["Data"])
                                        dataQueue.pop(index)
                                        index -= 1
                                        requestNum += 1
                                    index += 1

                    # 接收完毕，清除队列里的信息。
                    if len(dataQueue) != 0:
                        index = 0
                        while True:
                            if index >= len(dataQueue):
                                break
                            if dataQueue[index]["Num"] == requestNum:
                                f.write(dataQueue[index]["Data"])
                                dataQueue.pop(index)
                                index -= 1
                                requestNum += 1
                            index += 1
                    print("接收文件成功")
            except Exception as e:
                print(e)

        current_fileMD5 = calMD5ForFile('Received-' + FILE)
        MD5IsIn = 0
        for k, v in FILEandMD5.items():
            if k == FILE:
                MD5IsIn = 1
                break
        if MD5IsIn == 1:
            if current_fileMD5 != FILEandMD5[FILE]:
                print("经MD5检验，接收的文件不完整")
            else:
                print("经MD5检验，接收文件正确")
        else:
            print("未经MD5检验，无法验证文件完整性")

    return 0


############################ 更新资源 #####################################
def updatePeer():
    mainPeerClientSocket.sendto(UPDATE.encode('utf-8'), serverAddress)
    peerData = []
    peerData.append(str(PEER_DOWNLOAD_PORT))
    peerData += getFileList()
    mainPeerClientSocket.sendto('$'.join(peerData).encode('utf-8'), serverAddress)
    data, addr = mainPeerClientSocket.recvfrom(BUFFSIZE)
    if data.decode("utf-8") == UPDATE_SUCESSFULLY:
        print(UPDATE_SUCESSFULLY)
    else:
        print(UPDATE_FAILED + "，请先注册")
    return 0


############################ 删除节点 #####################################
def exitPeer():
    mainPeerClientSocket.sendto(EXIT.encode('utf-8'), serverAddress)
    data, addr = mainPeerClientSocket.recvfrom(BUFFSIZE)
    if data.decode() == DELETE_SUCCESSFULLY:
        print(DELETE_SUCCESSFULLY)
    else:
        print(DELETE_FAILED)
    return 0


######################## 传送文件给其他节点 #################################
def uploadSourceToPeer(Address, FILE, FLAG):
    if os.path.exists(filePath + FILE) is False:
        DownloadSocket.sendto(FILE_NO_EXIST.encode(), Address)
        print("文件不存在")
        return

    if FLAG == "0":  # 直接分组打包发送
        print("发送文件给其他节点中")
        try:
            count = 0
            f = open(filePath + FILE, 'rb')
            stat = os.stat(filePath + FILE)
            lenData = str(int(stat.st_size))
            DownloadSocket.sendto(lenData.encode(), Address)
            full_length = int(stat.st_size)
            now_length = 0
            print("正在发送文件 " + filePath + FILE)
            while True:
                if count == 0:
                    data = bytes(FILE, encoding='utf-8')
                    DownloadSocket.sendto(data, Address)
                data = f.read(1024)
                now_length += len(data)
                if str(data) != "b''":
                    progress_bar(now_length, full_length)
                    DownloadSocket.sendto(data, Address)
                else:
                    DownloadSocket.sendto('end'.encode('utf-8'), Address)
                    break
                count += 1
            f.close()
        except Exception as e:
            print(e)
            DownloadSocket.sendto(DOWNLOAD_FAILED.encode(), Address)
            print("\n发送文件失败")
        print("\n发送文件完毕")
    elif FLAG == "1":  # 发送奇数分组
        num = 0
        print("发送文件给其他节点中")
        DownloadSocket.sendto(DOWNLOAD_PREPARE.encode(), Address)
        try:
            with open(filePath + FILE, 'rb') as f:
                while True:
                    data = f.read(BUFFSIZE)
                    if num % 2 == 0:
                        num += 1
                        continue

                    if not data:
                        break
                    # 将二进制数据转换为base64字节码
                    base64_data_bytes = base64.b64encode(data)
                    # 字节码转换为字符串
                    base64_data_string = base64_data_bytes.decode('utf-8')
                    # 字符串存储在字典dict中
                    data_dict = dict(Num=num, Data=base64_data_string)
                    # 字典sdict转换为json
                    jsonData = json.dumps(data_dict)

                    DownloadSocket.sendto(jsonData.encode('utf-8'), Address)
                    num += 1  # 分组序号+1
            DownloadSocket.sendto((DOWNLOAD_SUCESSFULLY + "-1").encode("utf-8"), Address)
            print("发送文件完毕")
        except:
            DownloadSocket.sendto(DOWNLOAD_FAILED.encode('utf-8'), Address)
            print("发送文件失败")

    elif FLAG == "2":  # 发送偶数分组
        num = 0
        print("发送文件给其他节点中")
        DownloadSocket.sendto(DOWNLOAD_PREPARE.encode(), Address)
        try:
            with open(filePath + FILE, 'rb') as f:
                while True:
                    data = f.read(BUFFSIZE)
                    if num % 2 == 1:
                        num += 1
                        continue
                    if not data:
                        break
                    # 将数据转换为base64字节码,便于在json文本中存储
                    base64_data_bytes = base64.b64encode(data)
                    # 字节码转换为字符串
                    base64_data_string = base64_data_bytes.decode('utf-8')
                    # 字符串存储在字典dict中
                    data_dict = dict(Num=num, Data=base64_data_string)
                    # 字段dict转换为json
                    jsonData = json.dumps(data_dict)

                    DownloadSocket.sendto(jsonData.encode('utf-8'), Address)
                    num += 1
            DownloadSocket.sendto((DOWNLOAD_SUCESSFULLY + "-2").encode("utf-8"), Address)
            print("发送文件完毕")
        except:
            DownloadSocket.sendto(DOWNLOAD_FAILED.encode('utf-8'), Address)
            print("发送文件失败")

    if FLAG == "3":  # 大文件发送
        DownloadSocket.settimeout(500)
        print("发送文件给其他节点中")

        f = open(filePath + FILE, 'rb')
        stat = os.stat(filePath + FILE)
        lenData = str(int(stat.st_size))  # 发送文件大小
        DownloadSocket.sendto(lenData.encode(), Address)

        packet_count = 1  # 记录包的数目
        IS_RESEND = False  # 判断是否需要重传
        temp_pkt = ['tcp']  # 缓存上一个发送的包，用于重传

        while True:
            seq = packet_count
            ack = packet_count

            # 不需要重新传输
            if IS_RESEND == False:
                data = f.read(FILE_SIZE)
            # 重新传输
            else:
                seq -= 1
                ack -= 1
                packet_count -= 1
                data = temp_pkt[0]

            del temp_pkt[0]
            # 暂存此次要传输的文件内容
            temp_pkt.append(data)

            # 若文件没有传输完成
            if str(data) != "b''":
                end = 0
                DownloadSocket.sendto(pkt_struct.pack(*(seq, ack, end, data)), Address)
            # 文件传输完成
            else:
                end = 1
                data = 'end'.encode('utf-8')
                packet_count += 1
                DownloadSocket.sendto(pkt_struct.pack(*(seq, ack, end, data)), Address)

                # 等待接收方回复
                pkt_data, pkt_address = DownloadSocket.recvfrom(BUFFSIZE)
                unpkt_data = fb_struct.unpack(pkt_data)

                break

            packet_count += 1
            # 等待接收方回复
            pkt_data, pkt_address = DownloadSocket.recvfrom(BUFFSIZE)
            unpkt_data = fb_struct.unpack(pkt_data)

            ack = unpkt_data[0]
            rwnd = unpkt_data[1]
            # 根据接收方cwnd, 判断是否需要重新传送
            if rwnd == 0:
                IS_RESEND = True
            else:
                IS_RESEND = False

        f.close

    return 0


######################## 大文件下载 #################################
def bigDownloadSource(FILE, IP_LIST, PORT_LIST):
    goalAddr = []
    goalAddr.append(IP_LIST[0])
    goalAddr.append(PORT_LIST[0])
    goalAddressTuple = tuple(goalAddr)
    source = BIGDOWNLOAD + " " + FILE + " " + "3"

    try:
        mainPeerClientSocket.sendto(source.encode(), goalAddressTuple)
    except Exception as e:
        print(e)
    try:
        data, addr = mainPeerClientSocket.recvfrom(BUFFSIZE)
    except Exception as e:
        print(e)
    if data.decode() == FILE_NO_EXIST:
        print("文件不存在")
        return

    full_length = int(data.decode())
    now_length = 0
    print("正在下载")
    f = open(filePath + "Received-" + FILE, 'wb')
    start_time = time.clock()

    packet_count = 1  # 包的数目
    rwnd = 110  # 接收窗口
    temp_data = []  # 数据缓存

    while True:

        data, addr = mainPeerClientSocket.recvfrom(BUFFSIZE)
        try:
            unpkt_data = pkt_struct.unpack(data)  # 提取包数据
        except struct.error as e:
            break

        packet_count += 1  # 收到的包+1
        if rwnd > 0:
            # 若序列号为0，跳过该包
            # if unpkt_data[0] == 0: # seq
            #     mainPeerClientSocket.sendto(fb_struct.pack(*(unpkt_data[0], rwnd)), addr)
            #     continue
            # 如果序号不连续，丢弃该包
            if unpkt_data[1] != packet_count - 1:  # ack
                # 发回上一个包
                mainPeerClientSocket.sendto(fb_struct.pack(*(unpkt_data[1] - 1, rwnd)), addr)
                continue
            # 按序到达的包加入缓存
            temp_data.append(unpkt_data)
            rwnd -= 1
            # 接收完毕，返回 ack rwnd
            mainPeerClientSocket.sendto(fb_struct.pack(*(unpkt_data[0], rwnd)), addr)
        # 没有接收空间,cwnd = 0
        else:
            mainPeerClientSocket.sendto(fb_struct.pack(*(unpkt_data[0], rwnd)), addr)

        # 读缓存中的包
        while len(temp_data) > 0:
            unpkt_data = temp_data[0]
            seq = unpkt_data[0]
            ack = unpkt_data[1]
            fin = unpkt_data[2]
            data = unpkt_data[3]

            # 读取数据后删除，然后缓冲空间+1
            del temp_data[0]
            rwnd += 1

            # 写入数据，直到 fin = 1
            if fin != 1:
                now_length += len(data)
                if len(data) != FILE_SIZE:
                    print("nope")
                progress_bar(now_length, full_length)
                f.write(data)
            else:
                break

        # 文件结束
        if unpkt_data[2] == 1:  # fin
            break

    end_time = time.clock()
    full_time = end_time - start_time
    speed = full_length / (full_time * 1024 * 1024)
    speed = round(speed, 2)
    print('\n')
    print(now_length)
    print(full_length)
    print("\n下载完毕,下载速度为 " + str(speed) + " MB/s")
    f.close
    # checkMD5('Received-' + FILE)
    # current_fileMD5 = calMD5ForFile('Received-' + FILE)
    # print(calMD5ForFile('Received-1.mkv'))
    # MD5IsIn = 0
    # for k, v in FILEandMD5.items():
    #     if k == FILE:
    #         MD5IsIn = 1
    #         break
    # if MD5IsIn == 1:
    #     print(current_fileMD5)
    #     print(FILEandMD5[FILE])
    #     if current_fileMD5 != FILEandMD5[FILE]:
    #         print("经MD5检验，接收的文件不完整")
    #     else:
    #         print("经MD5检验，接收文件正确")


############################ 比较MD5 #####################################
def checkMD5(FILE):
    # print(FILEandMD5)
    current_fileMD5 = calMD5ForFile(FILE)
    FILE = FILE.split('-') #提取出原文件的文件名
    FILE = FILE[1]
    # print(FILE)
    # print(current_fileMD5)
    MD5IsIn = 0
    for k, v in FILEandMD5.items():#fileandmd5保存了{文件名：md5}格式的键值对
        if k == FILE:
            MD5IsIn = 1
            break
    if MD5IsIn == 1:
        if current_fileMD5 != FILEandMD5[FILE]:
            print("经MD5检验，接收的文件不完整")
        else:
            print("经MD5检验，接收文件正确")
    else:
        print("未经MD5检验，无法验证文件完整性")


############################ 下载请求线程 #####################################
def waitPeerToDownLoadSource():
    requestPeerQueue = []
    while True:
        try:
            data_upload, addr_upload = DownloadSocket.recvfrom(BUFFSIZE)
            if data_upload.decode("utf-8").split(' ')[0] == DOWNLOAD:
                print("其他节点请求资源中")
                temp_addr_upload = addr_upload
                temp_fileName_upload = data_upload.decode("utf-8").split(' ')[1]
                temp_flag_upload = data_upload.decode("utf-8").split(' ')[2]
                # 加入队列中
                requestPeerQueue.append(dict(addr=temp_addr_upload, fileName=temp_fileName_upload,
                                             flag=temp_flag_upload))
                if len(requestPeerQueue) != 0:
                    length = len(requestPeerQueue) - 1
                    uploadThread = threading.Thread(target=uploadSourceToPeer,
                                                    args=(requestPeerQueue[length]["addr"],
                                                          requestPeerQueue[length]["fileName"],
                                                          requestPeerQueue[length]["flag"]))
                    uploadThread.start()
            elif data_upload.decode("utf-8").split(' ')[0] == BIGDOWNLOAD:
                print("其他节点请求资源中")
                temp_addr_upload = addr_upload
                temp_fileName_upload = data_upload.decode("utf-8").split(' ')[1]
                temp_flag_upload = data_upload.decode("utf-8").split(' ')[2]
                server = Server(DownloadSocket)
                server.listen()
                # f = open(filePath + FILE, 'rb')
                # server.send_file(addr_upload[0],addr_upload[1],f,temp_fileName_upload)
                # uploadSourceToPeer(temp_addr_upload, temp_fileName_upload, temp_flag_upload)
            elif data_upload.decode("utf-8").split(' ')[0] == STREAM:
                streamReceive()
            elif data_upload.decode("utf-8").split(' ')[0] == BIGSTREAM:
                bigStreamReceive()
            else:
                print("非上传请求")
        except Exception as e:
            print(e)


############################ 发送视频流 #####################################
def streamSend(host, port):
    global streamEndFlag
    cap = cv2.VideoCapture(0)
    # 分辨率
    cap.set(3, 1280)
    cap.set(4, 720)
    addr = (host, int(port))
    mainPeerClientSocket.sendto(STREAM.encode(), addr)
    encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 50]
    while True:
        # 结束传输
        if streamEndFlag == 1:
            mainPeerClientSocket.sendto(''.encode(), addr)
            streamEndFlag = 0
            print("传输视频流结束")
            break
        ret, fra = cap.read()
        if ret:
            # 将图片格式转换(编码)成流数据，赋值到内存缓存中
            # 主要用于图像数据格式的压缩，方便网络传输
            # _, enfra = cv2.imencode('.jpg', fra)
            _, enfra = cv2.imencode('.jpg', fra, encode_param)
            print(len(enfra))
            mainPeerClientSocket.sendto(enfra, addr)
    cap.release()


############################ 接收视频流 #####################################
def streamReceive():
    import cv2
    cv2.namedWindow('stream')
    cv2.startWindowThread()
    while True:
        data, addr = DownloadSocket.recvfrom(400000)
        # 结束传输
        if data == b'':
            print("传输视频流结束")
            break
        data = np.frombuffer(data, dtype=np.uint8)
        if data[0]:
            imde = cv2.imdecode(data, 1)
            cv2.imshow('stream', imde)
            k = cv2.waitKey(1)
            if k == ord('q'):
                DownloadSocket.sendto(b'0', addr)
                break
    cv2.destroyAllWindows()


############################ 发送视频流（无压缩） #####################################
def bigStreamSend(host, port):
    global streamEndFlag
    cap = cv2.VideoCapture(0)
    # 不足360p
    cap.set(3, 640)
    cap.set(4, 480)
    # 图象质量
    encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 60]
    addr = (host, int(port))
    # ret, frame = cap.read()
    # 传输信号
    mainPeerClientSocket.sendto(BIGSTREAM.encode(), addr)
    while True:
        # 结束传输
        if streamEndFlag == 1:
            mainPeerClientSocket.sendto(''.encode(), addr)
            streamEndFlag = 0
            break
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break
        # 停止0.1S 防止发送过快服务的处理不过来，如果服务端的处理很多，那么应该加大这个值
        # time.sleep(0.01)
        ret, frame = cap.read()
        frame = cv2.flip(frame, 1)  # 水平翻转
        result, enfra = cv2.imencode('.jpg', frame, encode_param)
        s = frame.flatten().tostring()
        st = time.time()
        for i in range(20):
            time.sleep(0.001)
            # print(i.to_bytes(1, byteorder='big'))
            mainPeerClientSocket.sendto(s[i * 46080:(i + 1) * 46080] + i.to_bytes(1, byteorder='big'), addr)
        et = time.time()
        # print(et - st)
    cap.release()


############################ 接收视频流（无压缩） #####################################
def bigStreamReceive():
    import cv2
    cv2.namedWindow('stream')
    cv2.startWindowThread()
    bfsize = 46080
    chuncksize = 46081
    frame = np.zeros(bfsize * 20, dtype=np.uint8)
    cnt = 0
    while True:
        cnt += 1
        try:
            data, addr = DownloadSocket.recvfrom(chuncksize)
            if data == b'':
                break
            i = int.from_bytes(data[-1:], byteorder='big')
            line_data = np.frombuffer(data[:-1], dtype=np.uint8)
            frame[i * 46080:(i + 1) * 46080] = line_data
            if cnt == 20:
                cv2.imshow("frame", frame.reshape(480, 640, 3))
                cnt = 0

            if cv2.waitKey(1) & 0xFF == ord('q'):
                break
        except Exception as e:
            print(e)
    # cap.release()
    cv2.destroyAllWindows()


##########################################################################
############################ 大文件类 #####################################
##########################################################################


############################ 接收类 #####################################
class Client:

    def __init__(self, socket):
        # Create a socket for use
        # self.fileSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.fileSocket = socket
        self.MSSlen = 8000

        self.rtData = b""
        self.rtSEQ = 0
        self.rtACK = 0
        self.ACK = 0
        self.SEQ = 0

        self.drop_count = 0  # 丢包数
        self.lockForBuffer = threading.Lock()
        self.buffer = {}
        self.buffer_size = 20 * self.MSSlen

        self.rwnd = self.buffer_size
        self.rtrwnd = 0

        # the size of congestion window
        self.cwnd = 1 * self.MSSlen
        self.ssthresh = 256 * self.MSSlen

        # seg begin to send file
        self.beginSEQ = 0

        self.log = open("ReceiveLog.txt", "w")

        self.write_log_enable = 0

    def send_segment(self, SYN, ACK, SEQ, FUNC, serverName, port, data=b""):
        # use * to split
        self.fileSocket.sendto(b"%d*%d*%d*%d*%d*%b" % (SYN, ACK, SEQ, FUNC, self.rwnd, data), (serverName, port))
        # print("send segment to %s:%d --- SYN: %d ACK: %d SEQ: %d FUNC: %d rwnd: %d" % (serverName, port, SYN, ACK, SEQ, FUNC, self.rwnd))
        if self.write_log_enable == 1:
            self.log.write(
                "Send     to {0}:{1} --- Syn: {2} Ack: {3} Seq: {4} Rwnd: {5}\n".format(serverName, port, SYN, ACK, SEQ,
                                                                                        self.rwnd))

    def reliable_send_one_segment(self, SYN, FUNC, serverName, port, data=b""):
        dataComplete = False  # 结束标志
        delayTime = 1
        while not dataComplete:  # 文件未读取结束
            self.send_segment(SYN, self.ACK, self.SEQ, FUNC, serverName, port, data)
            self.fileSocket.settimeout(delayTime)  # 超时时间
            try:
                rtSYN, self.rtACK, self.rtSEQ, rtFUNC, self.rtrwnd, self.rtData, addr = self.receive_segment()

                # 建立连接
                if len(data) == 0 and self.rtACK == self.SEQ + 1:
                    dataComplete = True
                    self.SEQ = self.rtACK
                    self.ACK = self.rtSEQ + 1
                # 对方的ACK = 自己的SEQ + 数据长度，则数据传输完毕
                elif len(data) != 0 and self.rtACK == self.SEQ + len(data):
                    dataComplete = True
                    self.SEQ = self.rtACK
                    self.ACK = self.rtSEQ + len(self.rtData)

            # 没收到反馈，超时
            except socket.timeout as timeoutErr:
                delayTime *= 2  # 超时时间加倍
                self.drop_count += 1  # 丢包数加1
                if self.write_log_enable == 1:
                    self.log.write("Time out\n")
                # print(timeoutErr)

    def receive_segment(self):
        seg, addr = self.fileSocket.recvfrom(self.MSSlen + 100)
        SYN, ACK, SEQ, FUNC, rtrwnd = list(map(int, seg.split(b"*")[0:5]))  # 改为int类型
        data = seg[sum(map(len, seg.split(b"*")[0:5])) + 5:]
        # print("receive segment from %s:%d --- SYN: %d ACK: %d SEQ: %d FUNC: %d rtrwnd: %d" % (addr[0], addr[1], SYN, ACK, SEQ, FUNC, rtrwnd))
        if self.write_log_enable == 1 and FUNC != 2:
            self.log.write(
                "Rcvd from {0}:{1} --- Syn: {2} Ack: {3} Seq: {4} Rwnd: {5}\n".format(addr[0], addr[1], SYN, ACK, SEQ,
                                                                                      rtrwnd))
        return SYN, ACK, SEQ, FUNC, rtrwnd, data, addr

    def send_file(self, serverName, port, file, file_name):

        SYN = 0
        FUNC = 1

        # 文件分块，存入缓存
        data = []
        data_size = 0
        # print("begin to split file into MSS")
        while True:
            temp = file.read(self.MSSlen)
            data_size += len(temp)
            if temp == b'':
                break
            data.append(temp)
        ##print("finish split file into MSS")

        # 发送文件名和文件大小
        # print("send the file name and data size")
        self.reliable_send_one_segment(SYN, FUNC, serverName, port, b"%b %d" % (bytes(file_name, "UTF-8"), len(data)))

        # 发送文件
        # print("send the file")
        self.beginSEQ = self.SEQ  # beginSEQ = 0
        delay_time = 2  # 超时时间限
        self.cwnd = 1 * self.MSSlen  # 拥塞窗口
        self.ssthresh = 8 * self.MSSlen  # 门限
        fastACK = 0
        dupACKcount = 0  # 重复ack数量
        while True:
            init_time = time.time()

            # pipeline
            while (self.SEQ - self.rtACK) < min(self.cwnd, self.rtrwnd) \
                    and math.ceil((self.SEQ - self.beginSEQ) / self.MSSlen) < len(data):
                temp_data = data[(self.SEQ - self.beginSEQ) // self.MSSlen]
                self.send_segment(SYN, self.ACK, self.SEQ, 1, serverName, port, temp_data)
                self.SEQ += len(temp_data)

            # flow control
            if self.SEQ - self.rtACK >= self.rtrwnd:
                print("flow control")
                # check rwnd of the receiver
                self.send_segment(SYN, self.ACK, self.SEQ, 2, serverName, port, b"flow")

            # set timer
            self.fileSocket.settimeout(1)
            while True:
                try:
                    rtSYN, self.rtACK, rtSEQ, rtFUNC, self.rtrwnd, self.rtData, addr = self.receive_segment()
                    # 收到新的ack
                    if self.rtACK != fastACK:
                        self.cwnd = self.ssthresh
                        fastACK = self.rtACK
                        dupACKcount = 0
                    # 收到重复的ack
                    else:
                        dupACKcount += 1
                        # 3-ACK
                        if dupACKcount == 3:
                            self.ssthresh = self.cwnd / 2
                            self.cwnd = self.ssthresh + 3 * self.MSSlen
                except socket.timeout as timeoutErr:
                    pass
                if self.SEQ == self.rtACK:
                    # 慢开始阶段
                    if self.cwnd < self.ssthresh:
                        # print("slow start")
                        self.cwnd *= 2
                    # 拥塞避免阶段
                    else:
                        self.log.write("Congestion avoidance\n")
                        # print("congestion avoidance")
                        self.cwnd += 1 * self.MSSlen
                    self.SEQ = self.rtACK
                    break
                # 超时
                elif time.time() - init_time > delay_time:
                    self.drop_count += (1 + (self.SEQ - self.rtACK) // self.MSSlen)
                    self.log.write("Time out\n")
                    # print("time out")
                    self.SEQ = self.rtACK
                    self.ssthresh = self.cwnd / 2
                    self.cwnd = 1 * self.MSSlen
                    break

            # 传输结束
            if math.ceil((self.rtACK - self.beginSEQ) / self.MSSlen) == len(data):
                break

    def read_into_file(self, file_name, data_size):
        begin = 0
        end = data_size
        with open(filePath + "Received-" + file_name, 'wb') as file:
            while True:
                if begin == end:
                    # print("finish write to file successfully")
                    break

                while begin in self.buffer:
                    # if self.lockForBuffer.acquire():
                    #     #print("read from buffer write into file")
                    #     file.write(self.buffer[begin])
                    #     data_len = len(self.buffer[begin])
                    #     self.buffer.pop(begin)
                    #     self.rwnd += data_len
                    #     begin += 1
                    #     self.lockForBuffer.release()
                    file.write(self.buffer[begin])
                    data_len = len(self.buffer[begin])
                    self.buffer.pop(begin)
                    self.rwnd += data_len
                    begin += 1

    def receive_file(self, serverName, port, file_name):

        SYN = 0
        FUNC = 0

        # send file name to server for send
        # print("send the file name")
        print("正在下载...")
        self.log.write("Send file name\n")
        self.reliable_send_one_segment(SYN, FUNC, serverName, port, b"%b" % (bytes(file_name, "utf-8")))
        # print(type(self.rtData))
        data = self.rtData.decode().split('*')
        data_size = int(data[0])
        full_size = int(data[1])
        now_size = 0
        # data_size = int(self.rtData)
        # print(data_size)
        # print(full_size)
        # print("data size is %d" % data_size)

        self.beginACK = self.ACK
        self.lastACKRead = self.ACK

        file_thread = threading.Thread(target=self.read_into_file, args=(file_name, data_size,), name="fileThread")
        file_thread.start()

        begin = 0
        end = data_size

        while True:
            # 非阻塞模式下, 如果recv()调用没有发现任何数据或者send()调用无法立即发送数据,
            # 那么将引发socket.error异常
            self.fileSocket.setblocking(True)
            rtSYN, self.rtACK, self.rtSEQ, rtFUNC, rtrwnd, data, addr = self.receive_segment()
            if data == b"":
                print(self.ACK)
                self.ACK = self.rtSEQ + 1
            # do nothing , just control the flow
            elif rtFUNC == 2:
                # print("1 : " + str(self.ACK))
                self.ACK = self.rtSEQ
                # print("2 : " + str(self.rtSEQ))
            # 当需要的时候读入缓存
            elif self.ACK == self.rtSEQ:
                if self.lockForBuffer.acquire():
                    self.buffer[begin] = data
                    now_size += len(data)
                    begin += 1
                    progress_bar(now_size, full_size)
                    self.rwnd -= len(data)
                    self.ACK = self.rtSEQ + len(data)
                    self.lockForBuffer.release()
            # 回复
            self.send_segment(rtSYN, self.ACK, self.SEQ, rtFUNC, serverName, port)
            if begin == end:
                print("\n下载结束！")
                if self.write_log_enable == 1:
                    self.log.write("Receive file successfully\n")
                    self.log.flush()
                # print("finish receive file successfully")
                break

    def handshake(self, serverName, port):
        SYN = 1
        # syn = 1
        self.reliable_send_one_segment(SYN, 0, serverName, port)
        return True


############################ 发送类 #####################################
class Interface:

    def __init__(self, fileSocket, addr, ACK, SEQ, segments, lockForSeg):
        self.fileSocket = fileSocket
        self.addr = addr
        self.MSSlen = 8000

        self.rtSEQ = 0
        self.rtACK = 0
        self.ACK = ACK
        self.SEQ = SEQ

        self.segments = segments
        self.lockForSeg = lockForSeg

        self.drop_count = 0
        self.lockForBuffer = threading.Lock()
        self.buffer = {}
        self.buffer_size = 10 * self.MSSlen

        self.rwnd = self.buffer_size
        self.rtrwnd = 0

        # the size of congestion window
        self.cwnd = 1 * self.MSSlen
        self.ssthresh = 256 * self.MSSlen

        # seg begin to send file
        self.beginSEQ = 0

        self.has_construct = False

        self.beginI = 0

        self.log = open("SendLog.txt", "w")

        self.write_log_enable = 0

    def receive_segment(self, delayTime):
        init_time = time.time()
        while True:
            if self.beginI < len(self.segments):
                if self.lockForSeg.acquire():
                    seg = self.segments[self.beginI]
                    SYN, ACK, SEQ, FUNC, rtrwnd = list(map(int, seg.split(b"*")[0:5]))
                    data = seg[sum(map(len, seg.split(b"*")[0:5])) + 5:]

                    # print("receive segment from %s:%d --- SYN: %d ACK: %d SEQ: %d FUNC: %d rtrwnd: %d" %
                    #      (self.addr[0], self.addr[1], SYN, ACK, SEQ, FUNC, rtrwnd))
                    try:
                        if self.write_log_enable == 1:
                            self.log.write(
                                "Rcvd from {0}:{1} --- Syn: {2} Ack: {3} Seq: {4} Rwnd: {5}\n".format(self.addr[0],
                                                                                                      self.addr[1],
                                                                                                      SYN, ACK, SEQ,
                                                                                                      rtrwnd))
                    except Exception as e:
                        print(e)
                    self.lockForSeg.release()
                    self.beginI += 1
                    return SYN, ACK, SEQ, FUNC, rtrwnd, data
            elif time.time() - init_time > delayTime:
                raise socket.timeout

    def send_segment(self, SYN, ACK, SEQ, FUNC, data=b""):
        # * is the character used to split
        self.fileSocket.sendto(b"%d*%d*%d*%d*%d*%b" % (SYN, ACK, SEQ, FUNC, self.rwnd, data), self.addr)
        # print("send segment to %s:%d --- SYN: %d ACK: %d SEQ: %d FUNC: %d rwnd: %d" % (self.addr[0], self.addr[1], SYN, ACK, SEQ, FUNC, self.rwnd))
        try:
            if self.write_log_enable == 1 and FUNC != 2:
                self.log.write(
                    "Send     to {0}:{1} --- Syn: {2} Ack: {3} Seq: {4} Rwnd: {5}\n".format(self.addr[0], self.addr[1],
                                                                                            SYN,
                                                                                            ACK, SEQ, self.rwnd))
        except Exception as e:
            print(e)

    def reliable_send_one_segment(self, SYN, FUNC, data=b""):
        dataComplete = False
        delayTime = 1
        while not dataComplete:
            self.send_segment(SYN, self.ACK, self.SEQ, FUNC, data)
            try:
                rtSYN, self.rtACK, self.rtSEQ, rtFUNC, self.rtrwnd, rtData = self.receive_segment(delayTime)

                # TCP construction
                if len(data) == 0 and self.rtACK == self.SEQ + 1:
                    dataComplete = True
                    self.SEQ = self.rtACK
                    self.ACK = self.rtSEQ + 1
                # File data
                elif len(data) != 0 and self.rtACK == self.SEQ + len(data):
                    dataComplete = True
                    self.SEQ = self.rtACK
                    self.ACK = self.rtSEQ + len(data)

            except socket.timeout as timeoutErr:
                # double the delay when time out
                delayTime *= 2
                self.drop_count += 1
                if self.write_log_enable == 1:
                    self.log.write("Time out\n")
                print(timeoutErr)

    def read_into_file(self, file_name, data_size):
        begin = 0
        end = data_size
        with open(file_name, 'wb') as file:
            while True:
                if begin == end:
                    # print("finish write to file successfully")
                    break

                while begin in self.buffer:
                    if self.lockForBuffer.acquire():
                        # print("read from buffer write into file")
                        file.write(self.buffer[begin])
                        self.rwnd += len(self.buffer[begin])
                        self.buffer.pop(begin)
                        begin += 1
                        self.lockForBuffer.release()

    def receive_file(self, file_name, data_size):

        self.send_segment(0, self.ACK, self.SEQ, 1)

        self.beginACK = self.ACK
        self.lastACKRead = self.ACK

        file_thread = threading.Thread(target=self.read_into_file, args=(file_name, data_size,), name="fileThread")
        file_thread.start()

        begin = 0
        end = data_size
        while True:
            rtSYN, self.rtACK, self.rtSEQ, rtFUNC, rtrwnd, data = self.receive_segment(100)
            if data == b"":  # start
                self.ACK = self.rtSEQ + 1
            elif rtFUNC == 2:
                self.ACK = self.rtSEQ
            # write to the buffer only when receiver need
            elif self.ACK == self.rtSEQ:
                if self.lockForBuffer.acquire():
                    self.buffer[begin] = data
                    begin += 1
                    self.rwnd -= len(data)
                    self.ACK = self.rtSEQ + len(data)
                    self.lockForBuffer.release()
            # answer
            self.send_segment(rtSYN, self.ACK, self.SEQ, rtFUNC)
            if begin == end:
                print("finish receive file successfully")
                break

    def send_file(self, file_name):
        print(file_name)
        with open(filePath + file_name, 'rb') as file:
            # split file into MSS
            # data buffer
            data = []
            data_size = 0
            # print("begin to split file into MSS")
            while True:
                temp = file.read(self.MSSlen)
                data_size += len(temp)
                if temp == b'':
                    break
                data.append(temp)
            # print("finish")
            print("开始发送")
            # file length
            string = str(len(data)) + '*' + str(data_size)
            string = string.encode()
            # self.send_segment(0, self.ACK, self.SEQ, 0, b"%d*%d" % (len(data),data_size))
            # send file size and block num
            # self.log.write("Send file size\n")
            self.send_segment(0, self.ACK, self.SEQ, 0, string)

            # begin to send file
            # print("send the file")
            self.beginSEQ = self.SEQ
            delay_time = 2
            self.cwnd = 1 * self.MSSlen
            # self.ssthresh = 8 * self.MSSlen
            fastACK = 0
            dupACKcount = 0

            while True:
                init_time = time.time()

                # multi send
                # len of data less than window and not end
                # while (self.SEQ - self.rtACK) < min(self.cwnd, self.rtrwnd) \
                #         and math.ceil((self.SEQ - self.beginSEQ) / self.MSSlen) < len(data):
                #     temp_data = data[(self.SEQ - self.beginSEQ) // self.MSSlen]
                #     self.send_segment(0, self.ACK, self.SEQ, 1, temp_data)
                #     self.SEQ += len(temp_data)

                # send one
                if (self.SEQ - self.rtACK) < min(self.cwnd, self.rtrwnd) \
                        and math.ceil((self.SEQ - self.beginSEQ) / self.MSSlen) < len(data):
                    temp_data = data[(self.SEQ - self.beginSEQ) // self.MSSlen]
                    self.send_segment(0, self.ACK, self.SEQ, 1, temp_data)
                    self.SEQ += len(temp_data)

                # flow control
                # #if self.SEQ - self.rtACK >= self.rtrwnd:
                # if self.rtACK == 0:
                #     self.rtACK = 1

                if self.SEQ - self.rtACK >= self.rtrwnd:
                    # print("flow control")
                    # if self.write_log_enable == 1:
                    #     self.log.write("Flow control\n")
                    # check rwnd of the receiver
                    self.send_segment(0, self.ACK, self.SEQ, 2, b"flow")

                # timer and resend
                while True:
                    try:
                        rtSYN, self.rtACK, rtSEQ, rtFUNC, self.rtrwnd, self.rtData = self.receive_segment(1)
                        # a new ACK
                        if self.rtACK != fastACK:
                            # self.cwnd = self.ssthresh
                            fastACK = self.rtACK
                            dupACKcount = 0
                        else:
                            dupACKcount += 1
                            if dupACKcount == 3:
                                # if self.write_log_enable == 1:
                                #     self.log.write("3 Dup Ack\n")
                                self.ssthresh = self.cwnd / 2
                                self.cwnd = self.ssthresh + 3 * self.MSSlen
                    except socket.timeout as timeoutErr:
                        pass
                    if self.SEQ == self.rtACK:
                        if self.cwnd < self.ssthresh:
                            # print("slow start")
                            self.cwnd *= 2
                        else:
                            # print("congestion avoidance")
                            # if self.write_log_enable == 1:
                            #     self.log.write("Cogestion avoidance\n")
                            self.cwnd += 1 * self.MSSlen
                        # self.SEQ = self.rtACK
                        break
                    # elif time.time() - init_time > delay_time:
                    else:
                        self.drop_count += (1 + (self.SEQ - self.rtACK) // self.MSSlen)
                        # print("time out")
                        if self.write_log_enable == 1:
                            self.log.write("Time out\n")
                        self.SEQ = self.rtACK
                        self.ssthresh = self.cwnd / 2
                        self.cwnd = 1 * self.MSSlen
                        break

                # finish data transmission
                if math.ceil((self.rtACK - self.beginSEQ) / self.MSSlen) == len(data):
                    # print("File send successfully")

                    print("发送完毕")
                    print("丢包数为 " + str(self.drop_count))
                    try:
                        if self.write_log_enable == 1:
                            self.log.write("Send file successfully\n")
                            self.log.flush()
                    except Exception as e:
                        print(e)
                    break


############################ 处理类 #####################################
class Server:

    def __init__(self, socket):
        # Create a socket for use
        self.fileSocket = socket

        # self.fileSocket.bind((socket.gethostbyname(socket.gethostname()), 5555))
        # self.fileSocket.bind(("127.0.0.1", 5555))

        self.fileSocket.setblocking(True)

        self.addr_info = {}

        self.segmentsArr = {}

        self.lockForSeg = threading.Lock()

    def new_interface(self, addr, ACK, SEQ):
        #  New a buffer for the address
        self.segmentsArr[addr] = []
        clientInterface = Interface(self.fileSocket, addr, ACK, SEQ, self.segmentsArr[addr], self.lockForSeg)
        self.addr_info[addr] = clientInterface

    def delete_interface(self, addr):
        print("delete the interface")
        self.addr_info.pop(addr)

    def get_interface(self, addr):
        return self.addr_info[addr]

    def receive_segment(self):
        seg, addr = self.fileSocket.recvfrom(4096)
        if addr in self.addr_info and self.addr_info[addr].has_construct:
            if self.lockForSeg.acquire():
                self.segmentsArr[addr].append(seg)
                self.lockForSeg.release()
                return False, seg, addr
        return True, seg, addr

    def listen(self):
        while True:
            first, seg, addr = self.receive_segment()
            if not first:
                continue
            SYN, ACK, SEQ, FUNC, rtrwnd = list(map(int, seg.split(b"*")[0:5]))
            data = seg[sum(map(len, seg.split(b"*")[0:5])) + 5:]

            # TCP construction : SYN is 1
            if SYN == 1 and addr not in self.addr_info:

                # Second hand shake
                rtACK = SEQ + 1
                rtSEQ = 0

                self.new_interface(addr, rtACK, rtSEQ)
                self.get_interface(addr).send_segment(SYN, rtACK, rtSEQ, 0)

            # SYN is 0 and already TCP construction
            # FUNC 0 -- send file
            elif FUNC == 0 and addr in self.addr_info:
                file_name = data.split(b" ")[0].decode("UTF-8")
                print("send file %s to %s:%s" % (file_name, addr[0], addr[1]))
                self.get_interface(addr).SEQ = ACK
                self.get_interface(addr).ACK = SEQ + len(file_name)
                self.get_interface(addr).has_construct = True
                send_thread = threading.Thread(target=self.get_interface(addr).send_file,
                                               args=(file_name,))
                send_thread.start()

            # SYN is 0 and already TCP construction
            # FUNC 1 -- receive file
            elif FUNC == 1 and addr in self.addr_info:
                file_name = data.split(b" ")[0].decode("UTF-8")
                data_size = int(data.split(b" ")[1])
                print("receive file %s from %s:%s" % (file_name, addr[0], addr[1]))
                self.get_interface(addr).SEQ = ACK
                self.get_interface(addr).ACK = SEQ + len(data)
                self.get_interface(addr).has_construct = True
                receive_thread = threading.Thread(target=self.get_interface(addr).receive_file,
                                                  args=(file_name, data_size))
                receive_thread.start()

        fileSocket.close()


############################ 命令处理线程 #####################################
def cmdFuncThread():
    while True:
        cmd = input('''请输入指令:
        1. register -----------------------------------注册节点
        2. update -------------------------------------更新节点资源
        3. request ------------------------------------查询所有资源
        4. request [filename] -------------------------查询资源 filename
        5. download [filename] IP:PORT ----------------从 IP:PORT 下载 filename
        6. download [filename] IP1:PORT1 IP2:PORT2 ----从 IP1:PORT1 和 IP2:PORT2 下载 filename
        7. bigdownload [filename] IP:PORT -------------从 IP:PORT 下载大文件 filename
        8. stream IP:PORT -----------------------------向 IP:PORT 传输视频流
        9. bigstream IP:PORT --------------------------向 IP:PORT 传输视频流（无压缩方式）
        10. stop --------------------------------------停止传输视频流        
        11. exit --------------------------------------删除本节点信息\n''')
        if cmd == '':
            print("命令输入错误")
            continue
        cmd = cmd.split()
        if cmd[0] == REGISTER:  # 注册节点
            if len(cmd) > 1:
                print("命令输入错误")
                continue
            registerOnServer()
        elif cmd[0] == DOWNLOAD:  # 向其他节点下载资源
            if len(cmd) < 3 or len(cmd) > 4:
                print("命令输入错误")
                continue
            fileName = cmd[1]
            IP_DOWNLOAD_LIST = []
            PORT_DOWNLOAD_LIST = []
            IP_DOWNLOAD1 = cmd[2].split(":")[0]
            IP_DOWNLOAD_LIST.append(IP_DOWNLOAD1)
            PORT_DOWNLOAD1 = int(cmd[2].split(":")[1])
            PORT_DOWNLOAD_LIST.append(PORT_DOWNLOAD1)
            if len(cmd) == 4:
                IP_DOWNLOAD2 = cmd[3].split(":")[0]
                PORT_DOWNLOAD2 = int(cmd[3].split(":")[1])
                IP_DOWNLOAD_LIST.append(IP_DOWNLOAD2)
                PORT_DOWNLOAD_LIST.append(PORT_DOWNLOAD2)
            downloadSourceFromPeer(fileName, IP_DOWNLOAD_LIST, PORT_DOWNLOAD_LIST)
        elif cmd[0] == BIGDOWNLOAD:
            if len(cmd) != 3:
                print("命令输入错误")
                continue
            # fileName = cmd[1]
            # IP_DOWNLOAD_LIST = []
            # PORT_DOWNLOAD_LIST = []
            # IP_DOWNLOAD1 = cmd[2].split(":")[0]
            # IP_DOWNLOAD_LIST.append(IP_DOWNLOAD1)
            # PORT_DOWNLOAD1 = int(cmd[2].split(":")[1])
            # PORT_DOWNLOAD_LIST.append(PORT_DOWNLOAD1)
            # bigDownloadSource(fileName, IP_DOWNLOAD_LIST, PORT_DOWNLOAD_LIST)
            # checkMD5('Received-'+ fileName)
            file = cmd[1]
            data = cmd[2].split(':')
            host = data[0]
            port = int(data[1])
            source = BIGDOWNLOAD + " " + file + " " + "3"
            mainPeerClientSocket.sendto(source.encode(), (host, port))
            st = time.time()
            client = Client(mainPeerClientSocket)
            client.handshake(host, port)
            client.receive_file(host, port, file)
            et = time.time()
            ft = round((et - st), 2)
            print("用时 " + str(ft) + " s")
            checkMD5("Received-" + file)
        elif cmd[0] == UPDATE:  # 更新节点
            if len(cmd) > 1:
                print("命令输入错误")
                continue
            updatePeer()
        elif cmd[0] == REQUEST:  # 请求资源
            if len(cmd) > 2:
                print("命令输入错误")
                continue
            if len(cmd) == 2:
                requestPeerSource(cmd[1])
            else:
                requestAllSource()
        elif cmd[0] == EXIT:  # 删除节点
            if len(cmd) > 1:
                print("命令输入错误")
                continue
            exitPeer()
        elif cmd[0] == STREAM:
            if len(cmd) != 2:
                print("命令输入错误")
                continue
            data = cmd[1].split(':')
            host = data[0]
            port = data[1]
            streamThread = threading.Thread(target=streamSend, args=(host, port))
            streamThread.start()
        elif cmd[0] == BIGSTREAM:
            if len(cmd) != 2:
                print("命令输入错误")
                continue
            data = cmd[1].split(':')
            host = data[0]
            port = data[1]
            print("正在向 %s:%s 传输视频流" % (host, port))
            streamThread = threading.Thread(target=bigStreamSend, args=(host, port))
            streamThread.start()
        elif cmd[0] == STOP:
            if len(cmd) != 1:
                print("命令输入错误")
                continue
            global streamEndFlag
            streamEndFlag = 1
        else:
            print("未知命令")


if __name__ == '__main__':
    t1 = threading.Thread(target=cmdFuncThread)
    t2 = threading.Thread(target=waitPeerToDownLoadSource)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
