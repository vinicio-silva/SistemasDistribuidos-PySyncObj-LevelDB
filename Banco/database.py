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
        self.banco = './bancos/'+ str(porta) + '/'

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
                
        if number == 1:
            self.portaSocket = 1050
            self.replica = Banco(self.portaSocket,'localhost:1000',['localhost:1100', 'localhost:1200'])
        if number == 2:
            self.portaSocket = 1060
            self.replica = Banco(self.portaSocket,'localhost:1100',['localhost:1000', 'localhost:1200'])
        if number == 3:
            self.portaSocket = 1070
            self.replica = Banco(self.portaSocket,'localhost:1200',['localhost:1100', 'localhost:1000'])

    def socket(self):
        sock = socket.socket()
        host = socket.gethostname()
        sock.bind((host, self.portaSocket))
        sock.listen(30)
        while True:
            conn, addr = sock.accept()
            threading.Thread(target=self.controller, args=(conn, addr)).start()
            
    def controller(self, conn, addr):
        while True:
            data = conn.recv(16480)
            msg = data.decode()
            if msg:
                responseMsg = json.loads(msg)
                functionName = responseMsg['function']
                key = responseMsg['key']
                value = responseMsg['value']
                
            if functionName == 'inserir':
                self.replica.insertData(key,value)
                resp = json.dumps({'msg': "Operacao realizada"})
            if functionName == 'leitura':
                print(key)
                response = self.replica.getData(key)
                print(response)
                resp = json.dumps({'msg': "Operacao realizada", 'data': response})
                
            if functionName == 'deletar':
                self.replica.deleteData()
                resp = json.dumps({'msg': "Operacao realizada"})

            conn.send(resp.encode())

def main():
    if len(sys.argv) < 2:
        sys.exit(-1)

    number = int(sys.argv[1])

    if number != 1 and number != 2 and number != 3:
        print("Número deve ser 1, 2 ou 3")
    else:
        replica = Setup(number)
        replica.socket()

if __name__ == '__main__':
    main()