from __future__ import print_function
import plyvel
import sys
sys.path.append("../")
from pysyncobj import SyncObj, replicated
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
    
    def getMultipeData(self, key, value):
        db = plyvel.DB(self.banco, create_if_missing=True)
        object = []        
        val = value.replace('"','')
        for index, item in db:
            chave = index.decode()
            chaveValue = chave.split(':')[1]
            if val in chave:
                dados =  json.loads(item.decode())
                valueSplit = key.split(':')[1]
                if dados != None and dados['clientId'] == str(valueSplit):
                    object.append([{'Ordem Id': chaveValue,'Dados': dados}])
        db.close()
        return object

class Setup():
    
    def __init__(self, arg):
                
        if arg == 1:
            self.portaSocket = 1050
            self.replica = Banco(self.portaSocket,'localhost:1000',['localhost:1100', 'localhost:1200'])
        if arg == 2:
            self.portaSocket = 1060
            self.replica = Banco(self.portaSocket,'localhost:1100',['localhost:1000', 'localhost:1200'])
        if arg == 3:
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
                self.replica.insertData(key, value)
                resp = json.dumps({'msg': "Operacao realizada"})
            if functionName == 'leitura':
                response = self.replica.getData(key)
                resp = json.dumps({'msg': "Operacao realizada", 'data': response})
            if functionName == 'leituraMultipla':
                response = self.replica.getMultipeData(key, value)
                resp = json.dumps({'msg': "Operacao realizada", 'data': response})   
            if functionName == 'deletar':
                self.replica.deleteData(key)
                resp = json.dumps({'msg': "Operacao realizada"})

            conn.send(resp.encode())

def main():
    if len(sys.argv) < 2:
        sys.exit(-1)

    arg = int(sys.argv[1])

    if arg != 1 and arg != 2 and arg != 3:
        print("Valor do argumento deve ser 1, 2 ou 3")
    else:
        replica = Setup(arg)
        replica.socket()

if __name__ == '__main__':
    main()