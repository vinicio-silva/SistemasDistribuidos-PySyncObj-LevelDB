from __future__ import print_function
import plyvel
import sys
sys.path.append("../")
from pysyncobj import SyncObj, SyncObjConf, replicated
import socket
import threading
import json


class Banco(SyncObj):
    def __init__(self, porta, primaria, secundaria):
        super(Banco, self).__init__(primaria, secundaria)
        self.banco = './Banco/bancos/'+ str(porta) + '/'

    @replicated
    def insertData(self, key, value):
        db = plyvel.DB(self.banco, create_if_missing=True)
        chaveBytes = bytes(key, 'utf-8')
        valorBytes = bytes(value,'utf-8')
        db.put(chaveBytes, valorBytes)
        db.close()
        
    @replicated
    def deleteData(self, key):
        db = plyvel.DB(self.banco, create_if_missing=True)
        chaveBytes = bytes(key, 'utf-8')
        db.delete(chaveBytes)
        db.close()

    def getData(self, key):
        db = plyvel.DB(self.banco, create_if_missing=True)
        chaveBytes = bytes(key, 'utf-8')
        respBytes = db.get(chaveBytes)
        resp = None if not respBytes else respBytes.decode()
        db.close()
        return resp

class Setup():
    
    def __init__(self, number):
                
        if number == 0:
            self.portaSocket = 1050
            self.replica = Banco(self.portaSocket,'localhost:1000',['localhost:1100', 'localhost:1200'])
        if number == 1:
            self.portaSocket = 1060
            self.replica = Banco(self.portaSocket,'localhost:1100',['localhost:1000', 'localhost:1200'])
        if number == 2:
            self.portaSocket = 1070
            self.replica = Banco(self.portaSocket,'localhost:1200',['localhost:1100', 'localhost:1000'])

    def socket(self):
        sock = socket.socket()
        host = socket.gethostname()
        sock.bind((host, self.portaSocket))
        sock.listen(30)
        print('iniciando')
        while True:
            conn, addr = sock.accept()
            threading.Thread(target=self.controller, args=(conn, addr)).start()
            
    def controller(self, conn, addr):
        while True:
            data = conn.recv(16480)
            msg = data.encode()
            if msg:
                requestMsg = json.loads(msg)
                functionName = requestMsg['functionName']
                key = requestMsg['key']
                value = requestMsg['value']
                
            if functionName == 'inserir':
                self.replica.insertData(key,value)
                resp = {'msg': "Operação realizada"}
                
            if functionName == 'leitura':
                self.replica.getData()
                resp = {'msg': "Operação realizada"}
                
            if functionName == 'deletar':
                self.replica.deleteData()
                resp = {'msg': "Operação realizada"}
                
            conn.send(resp.encode())

def main():
    if len(sys.argv) < 2:
        sys.exit(-1)

    number = int(sys.argv[1])

    if number != 0 and number != 1 and number != 2:
        print("Número deve ser 0, 1 ou 2")
    else:
        replica = Setup(number)
        replica.socket()

if __name__ == '__main__':
    main()