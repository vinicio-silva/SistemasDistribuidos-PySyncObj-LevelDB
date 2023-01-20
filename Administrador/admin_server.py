import ast
from concurrent import futures
import grpc
import admin_pb2
import admin_pb2_grpc
from paho.mqtt import client as mqtt
import json
import plyvel
import socket
from cache import Cache
import sys
import random

def requestReplica (function, key, value=None):
    global CacheAux, Socket
    if (function == 'leitura'):
        cache = CacheAux.read(key)
        if cache != None:
            return {'data': cache}
    requestMsg = json.dumps({'function': function, 'key': key, 'value': json.dumps(value)})
    resp = None
    Socket.send(requestMsg.encode())
    resp = Socket.recv(16480)
    response = json.loads(resp.decode())
    if response['msg'] == 'Operacao realizada':
        if function == 'inserir':
            CacheAux.insert(key, value)
        if function == 'leitura':
            if response['data'] != None:
                CacheAux.insert(key, response['data'])
        if function == 'deletar':
            CacheAux.insert(key, None)
    return response

def validateReplica():
    global Socket
    notFound = 0
    for i in range (1,3):
        try:
            Socket = socket.socket()
            Socket.settimeout(0.1)
            host = socket.gethostname()
            if i == 1:
                Socket.connect((host, 1050))
                print('Replica ' + str(i) + ' escolhida!')
                break
            if i == 2:
                Socket.connect((host, 1060))
                print('Replica ' + str(i) + ' escolhida!')
                break
            if i == 3:
                Socket.connect((host, 1070))
                print('Replica ' + str(i) + ' escolhida!')
                break            

        except BaseException as e:     
            notFound = notFound+1
    
    if notFound >= 2:
        print('Replicas indisponiveis, fechando o programa')
        sys.exit()
class AdminServicer(admin_pb2_grpc.AdminServicer):  
    def inserirCliente(self, request_iterator, context):        
        print("Inserir Cliente")
        reply = admin_pb2.inserirClienteReply()
        key = str('clientId:' + request_iterator.clientId)
        value = request_iterator.dadosCliente
        resp = requestReplica('leitura', key)
        if resp['data'] != None:
            reply.message = 'Cliente já existe!'         
        else: 
            resp2 = requestReplica('inserir', key, value)
            if resp2['msg'] == 'Operacao realizada':
                print("Operacao realizada")
                reply.message = 'Cliente inserido!'

        return reply
    # def modificarCliente(self, request_iterator, context):
    #     db1,db2,db3 = startDatabase()
    #     print("Modificar Cliente")      
    #     reply = admin_pb2.modificarClienteReply()        
    #     if getData(db1, str('clientId:' + request_iterator.clientId)) == None:
    #         reply.message = 'Cliente não existe!'         
    #     else: 
    #         novosDados = json.loads(request_iterator.dadosCliente)
    #         dadosCliente = {"nome": novosDados['nome'], "sobrenome": novosDados['sobrenome']}
    #         insertData(db1,str('clientId:' + request_iterator.clientId), json.dumps(dadosCliente))
    #         resp = getData(db1, str('clientId:' + request_iterator.clientId))
    #         print("Modificação realizada: " + resp)
    #         reply.message = 'Cliente modificado!'

    #     return reply
    def recuperarCliente(self, request_iterator, context):
        print("Recuperar Cliente")
        reply = admin_pb2.recuperarClienteReply()
        key = str('clientId:' + request_iterator.clientId)
        resp = requestReplica('leitura', key)

        if resp['data'] == None:
            reply.message = 'Cliente não existe!'         
        else: 
            dadosCliente = resp['data']
            reply.message = "Cliente recuperado:" + dadosCliente

        return reply
    # def apagarCliente(self, request_iterator, context):
    #     db1,db2,db3 = startDatabase()
    #     print("Apagar Cliente")

    #     reply = admin_pb2.apagarClienteReply()
        
    #     if getData(db1, str('clientId:' + request_iterator.clientId)) == None:
    #         reply.message = 'Cliente não existe!'         
    #     else: 
    #         deleteData(db1, str('clientId:' + request_iterator.clientId))
    #         print("Cliente apagado")
    #         reply.message = 'Cliente apagado!'

    #     return reply
    # def inserirProduto(self, request_iterator, context):
    #     db1,db2,db3 = startDatabase()
    #     print("Inserir Produto")
    #     reply = admin_pb2.inserirProdutoReply()
    #     if getData(db1, str('produtoId:' + request_iterator.produtoId)):
    #         reply.message = 'Produto já existe!'         
    #     else: 
    #         insertData(db1,str('produtoId:' + request_iterator.produtoId), request_iterator.dadosProduto)            
    #         resp = getData(db1, str('produtoId:' + request_iterator.produtoId))
    #         print("Cadastro realizada:" + resp)
    #         reply.message = 'Produto cadastrado!'

    #     return reply
    # def modificarProduto(self, request_iterator, context):
    #     db1,db2,db3 = startDatabase()
    #     print("Modificar Produto")      
    #     reply = admin_pb2.modificarProdutoReply()
        
    #     if getData(db1, str('produtoId:' + request_iterator.produtoId)) == None:
    #         reply.message = 'Produto não existe!'         
    #     else: 
    #         novosDados = json.loads(request_iterator.dadosProduto)
    #         dadosProduto = {"nome": novosDados['nome'], "quantidade": novosDados['quantidade'], "preco": novosDados['preco']}
    #         insertData(db1,str('produtoId:' + request_iterator.produtoId), json.dumps(dadosProduto))            
    #         resp = getData(db1, str('produtoId:' + request_iterator.produtoId))
    #         print("Modificação realizada:" + resp)
    #         reply.message = 'Produto modificado!'

    #     return reply
    # def recuperarProduto(self, request_iterator, context):
    #     db1,db2,db3 = startDatabase()
    #     print("Recuperar Produto")

    #     reply = admin_pb2.recuperarProdutoReply()
        
    #     if getData(db1, str('produtoId:' + request_iterator.produtoId)) == None:
    #         reply.message = 'Produto não existe!'         
    #     else: 
    #         resp = getData(db1, str('produtoId:' + request_iterator.produtoId))
    #         print("Recuperação realizada:" + resp)
    #         dadosProduto = json.loads(resp)
    #         reply.message = f"Produto recuperado:\nNome - {dadosProduto['nome']}\nQuantidade - {dadosProduto['quantidade']}\nPreço - {dadosProduto['preco']}"

    #     return reply
    # def apagarProduto(self, request_iterator, context):
    #     db1,db2,db3 = startDatabase()
    #     print("Apagar Produto")

    #     reply = admin_pb2.apagarProdutoReply()
        
    #     if getData(db1, str('produtoId:' + request_iterator.produtoId)) == None:
    #         reply.message = 'Produto não existe!'         
    #     else: 
    #         deleteData(db1, str('produtoId:' + request_iterator.produtoId))
    #         print("Produto apagado")
    #         reply.message = 'Produto apagado!'

    #     return reply

    
def serve():
    porta = input("Digite uma porta para abrir o servidor: ")    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    admin_pb2_grpc.add_AdminServicer_to_server(AdminServicer(), server)
    server.add_insecure_port(f"localhost:{porta}")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    CacheAux = Cache()
    Socket = None
    validateReplica()
    serve()