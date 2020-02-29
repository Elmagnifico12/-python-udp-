import socket
import os
import sqlite3
import time
import json

######################################################################
############################## 全局变量 ##############################
######################################################################

BUFFSIZE = 2048  # 传输文件的缓冲区大小
filePath = os.path.abspath('.')  # 当前目录的绝对路径
dataBaseName = 'peerInfo.sqlite'  # 数据库的名字
SERVER_IP = "127.0.0.1"  # 服务器ip地址 192.168.1.105  127.0.0.1
SERVER_PORT = 8080  # 服务器端口号

########################################################################
############################## 通信指令 #################################
########################################################################

REGISTER = "register"  # 请求服务器注册peer
REQUEST = "request"  # 请求服务器返回资源对应的peer列表
UPDATE = "update"  # 更新peer资源信息
DOWNLOAD = "download"  # 请求peer下载资源命令
UPLOAD = "upload"  # 上传资源给其他peer
EXIT = "exit"  # 退出客户端
REGISTER_SUCESSFULLY = "节点注册成功"  # 注册成功
REGISTER_FAILED = "节点注册失败"  # 注册失败
HAVE_REGISTERED = "该节点已经注册，无需重复注册"  # 重复注册
UPDATE_SUCESSFULLY = "节点更新资源成功"  # 更新成功
UPDATE_FAILED = "节点更新资源失败"  # 更新失败
SEND_SUCESSFULLY = "发送成功"  # 发送成功
SEND_FAILED = "发送失败"  # 发送失败
DELETE_PEER = "删除节点信息成功"  # 删除节点成功
DOWNLOAD_SUCESSFULLY = "下载成功"  # 下载成功
DOWNLOAD_FAILED = "下载失败"  # 下载失败
SERVER_ERROR = "服务器错误"  # 服务器错误

#########################################################################
############################ 服务器SOCKET ################################
#########################################################################

serverAddress = (SERVER_IP, SERVER_PORT)
serverSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
serverSocket.bind(serverAddress)
print("服务器绑定成功")

############################################################################
########################## SQLITE3 数据库 ###################################
############################################################################

########################## 数据库表格结构 ###################################

"""
        Table PeerInfo 节点信息表

    |   IP     |   PORT   | REGISTER_TIMESTAMP | DOWNLOAD_PORT |
    -------------------------------------------------------------
    | 节点地址 | 节点端口 |    注册时间        |   下载端口    |

        Table PeerSource 资源信息表

 |   IP    |   DOWNLOAD_PORT  |    FILE    |     SIZE      |  FILEMD5  |  REGISTER_FLAG  | UPDATE_TIMESTAMP
 ----------------------------------------------------------------------------------------------------------
 |  下载IP |     下载端口     | 文件资源名 | 文件大小（MB）| 文件MD5值 |  节点注册标志   |  资源更新时间

"""

############################ 连接数据库 #####################################

conn = sqlite3.connect(filePath + dataBaseName)  # 连接数据库，创建connection对象，若数据库不存在，则创建一个新的数据库
print("数据库连接成功")
cur = conn.cursor()  # 创建游标cursor对象
try:
    cur.execute(
        '''Create TABLE PeerInfo (IP text, PORT integer, REGISTER_TIMESTAMP timestamp, DOWNLOAD_PORT integer );''')
    cur.execute(
        '''Create TABLE PeerSource(IP text, DOWNLOAD_PORT integer, FILE VARCHAR(20), SIZE FLOAT(2), FILEMD5 text,REGISTER_FLAG BOOLEAN, UPDATE_TIMESTAMP timestamp);''')
    print("表格构建成功")
except:
    print("表格构建成功")
conn.commit()  # 提交当前事务，不使用时为放弃所做的修改，即不保存


############################ 更新数据库节点资源 #####################################
def updatePeerSQL(IPAddress, DOWNLOAD_Port, File, FileSize, FileMD5):
    registerFlag = True  # 节点注册标志
    updateTimeStamp = time.strftime('%Y-%m-%d %H:%M:%S')  # 资源更新时间
    Sql = "insert into PeerSource values (?, ?, ?, ?, ?, ?, ?);"
    cur.execute(Sql, (IPAddress, DOWNLOAD_Port, File, FileSize, FileMD5, registerFlag, updateTimeStamp))
    print("更新资源：" + File)
    conn.commit()
    return True


############################ 查找资源 #####################################
def requestSQL(FileSourceName):
    sql = "select * from PeerSource where FILE = ?"
    sourceList = cur.execute(sql, (FileSourceName,)).fetchall()
    return sourceList


############################ 查询重复注册 #####################################
def checkRegister(host, port):
    sql = "select * from PeerInfo where " + "IP = \'" + host + "\' AND PORT = \'" + str(port) + "\';"
    registerList = cur.execute(sql).fetchall()
    if not registerList:
        return False
    else:
        return True


############################ 删除节点信息 #####################################
def deletePeerSQL(host, port):
    Sql = "delete from PeerInfo where IP = \'" + host + "\' AND PORT = \'" + str(port) + "\';"
    cur.execute(Sql)  # 删除节点信息表
    Sql = "delete from PeerSource where IP = \'" + host + "\' AND DOWNLOAD_PORT = \'" + str(port) + "\';"
    cur.execute(Sql)  # 删除资源信息表
    conn.commit()
    return False


############################ 加入节点信息 #####################################
def registerPeerSQL(host, port, DOWNLOAD_PORT):
    Sql = "insert into PeerInfo values(?,?,?,?)"
    registerTimeStamp = time.strftime('%Y-%m-%d %H:%M:%S')
    cur.execute(Sql, (host, port, registerTimeStamp, DOWNLOAD_PORT))
    conn.commit()


############################################################################
########################### 信息传输函数 ####################################
############################################################################

############################ 注册节点 #####################################
def registerPeer(peerAddr):
    data, addr = serverSocket.recvfrom(BUFFSIZE)
    fileList = ((data.decode('utf-8')).split("$"))  # 转换为json列表
    registerFlag = checkRegister(addr[0], str(addr[1]))
    try:
        if not registerFlag:
            registerPeerSQL(addr[0], addr[1], fileList[0])  # host port DOWNLOAD_PORT
            for fileJson in fileList[1:]:
                jsonDict = json.loads(fileJson)
                updatePeerSQL(addr[0], int(fileList[0]), jsonDict["fileName"], float(jsonDict["fileSize"]),
                              jsonDict["fileMD5"])
            conn.commit()
            serverSocket.sendto(REGISTER_SUCESSFULLY.encode('utf-8'), addr)
            print("节点 " + str(peerAddr) + " 注册成功")
        else:
            print("节点 " + str(peerAddr) + " 已注册")
            serverSocket.sendto(HAVE_REGISTERED.encode('utf-8'), addr)
    except:
        serverSocket.sendto(REGISTER_FAILED.encode('utf-8'), addr)
        print("节点 " + str(peerAddr) + " 注册过程发生错误")


############################ 发送节点信息 #####################################
def requestPeer(FileSourceName, peerAddr):
    try:
        List = requestSQL(FileSourceName)
        data = ''
        for i in range(0, len(List)):
            data += str(List[i]) + '$'
        serverSocket.sendto(data.encode('utf-8'), peerAddr)
        print("资源列表如下：")
        print('   IP地址 ', ' 下载端口', '  资源名称', '  文件大小', '            MD5值      ', '       是否注册', '   最后更新时间 ')
        for i in range(0, len(List)):
            print(List[i])
        print("向节点" + str(peerAddr) + "发送资源列表成功")
    except:
        serverSocket.sendto(SEND_FAILED.encode('utf-8'), peerAddr)
        print("向节点" + str(peerAddr) + "发送资源列表失败")


############################ 更新节点 #####################################
def updatePeer(peerAddress):
    data, addr = serverSocket.recvfrom(BUFFSIZE)
    fileList = (((data.decode('utf-8')).split("$")))
    deleteSql = "delete from PeerSource  where IP =  \'" + addr[0] + "\' AND DOWNLOAD_PORT = \'" + fileList[0] + "\';"
    cur.execute(deleteSql)  # 删除旧节点
    try:
        for fileJson in fileList[1:]:
            jsonDict = json.loads(fileJson)
            updatePeerSQL(addr[0], int(fileList[0]), jsonDict["fileName"], float(jsonDict["fileSize"]),
                          jsonDict["fileMD5"])
        serverSocket.sendto(UPDATE_SUCESSFULLY.encode('utf-8'), addr)
        print("节点 " + str(peerAddress) + " 更新成功")
    except:
        serverSocket.sendto(UPDATE_FAILED.encode('utf-8'), addr)
        print("节点 " + str(peerAddress) + " 更新失败")
    return 0


############################ 删除节点 #####################################
def deletePeer(host, port):
    try:
        deletePeerSQL(host, port)
        serverSocket.sendto(DELETE_PEER.encode('utf-8'), (host, port))
        print("节点 " + str(host) + ":" + str(port) + " 删除成功")
    except:
        print("节点 " + str(host) + ":" + str(port) + " 删除失败")


############################################################################
########################### 接收用户命令 ####################################
############################################################################
if __name__ == '__main__':
    text = []
    print("开始接收节点命令")
    print("--------------------------------")
    while True:
        peerData, peerAddr = serverSocket.recvfrom(BUFFSIZE)
        cmd = peerData.decode('utf-8')
        text = cmd.split()
        print("命令 " + str(cmd) + " 来自节点 " + str(peerAddr))
        if text[0] == REGISTER:
            print("--------------------------------")
            registerPeer(peerAddr)
            print("--------------------------------")
        elif text[0] == REQUEST:
            print("--------------------------------")
            requestPeer(text[1], peerAddr)
            print("--------------------------------")
        elif text[0] == UPDATE:
            print("--------------------------------")
            if checkRegister(peerAddr[0], peerAddr[1]):
                updatePeer(peerAddr)
            else:
                print("节点 " + str(peerAddr) + " 未注册")
                info = "请先注册"
                serverSocket.sendto(info.encode('utf-8'), peerAddr)
            print("--------------------------------")
        elif text[0] == EXIT:
            print("--------------------------------")
            deletePeer(peerAddr[0], peerAddr[1])
            print("--------------------------------")

