import ast
from concurrent import futures
import grpc
import admin_pb2
import admin_pb2_grpc
from paho.mqtt import client as mqtt
import json
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
    requestMsg = json.dumps({'function': function, 'key': key,  'value': json.dumps(value)})
    resp = None
    Socket.send(requestMsg.encode())
    resp = Socket.recv(16480)
    response = json.loads(resp.decode())

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
    while (True):
        i = random.randint(1,3)
        try:
            Socket = socket.socket()
            Socket.settimeout(1)
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
    
        if notFound >= 10:
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
            requestReplica('inserir', key, value)
            reply.message = 'Cliente inserido!'

        return reply
    def modificarCliente(self, request_iterator, context):
        print("Modificar Cliente")
        reply = admin_pb2.modificarClienteReply()
        key = str('clientId:' + request_iterator.clientId)
        value = request_iterator.dadosCliente
        resp = requestReplica('leitura', key)
        if resp['data'] == None:
            reply.message = 'Cliente não existe!'
        else:
            requestReplica('deletar', key)
            requestReplica('inserir', key, value)
            resp = requestReplica('leitura',key)
            print("Modificação realizada:")
            print(resp)
            reply.message = 'Cliente modificado!'

        return reply
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
    def apagarCliente(self, request_iterator, context):
        print("Apagar Cliente")
        reply = admin_pb2.apagarClienteReply()
        key = str('clientId:' + request_iterator.clientId)
        resp = requestReplica('leitura', key)
        if resp['data'] == None:
            reply.message = 'Cliente não existe!'
        else: 
            requestReplica('deletar', key)
            reply.message = 'Cliente deletado!'

        return reply
    def inserirProduto(self, request_iterator, context):
        print("Inserir Produto")
        reply = admin_pb2.inserirProdutoReply()
        key = str('produtoId:' + request_iterator.produtoId)
        value = request_iterator.dadosProduto
        resp = requestReplica('leitura', key)
        if resp['data'] != None:
            reply.message = 'Produto já existe!'         
        else: 
            requestReplica('inserir', key, value)
            reply.message = 'Produto inserido!'

        return reply
    def modificarProduto(self, request_iterator, context):
        print("Modificar Produto")
        reply = admin_pb2.modificarProdutoReply()
        key = str('produtoId:' + request_iterator.produtoId)
        value = request_iterator.dadosProduto
        resp = requestReplica('leitura', key)
        if resp['data'] == None:
            reply.message = 'Produto não existe!'
        else:
            requestReplica('deletar', key)
            requestReplica('inserir', key, value)
            resp = requestReplica('leitura',key)
            print("Modificação realizada:")
            print(resp)
            reply.message = 'Produto modificado!'

        return reply
    def recuperarProduto(self, request_iterator, context):
        print("Recuperar Produto")
        reply = admin_pb2.recuperarProdutoReply()
        key = str('produtoId:' + request_iterator.produtoId)
        resp = requestReplica('leitura', key)

        if resp['data'] == None:
            reply.message = 'Produto não existe!'         
        else: 
            dadosProduto = resp['data']
            reply.message = "Produto recuperado:" + dadosProduto

        return reply
    def apagarProduto(self, request_iterator, context):
        print("Apagar Produto")
        reply = admin_pb2.apagarProdutoReply()
        key = str('produtoId:' + request_iterator.produtoId)
        resp = requestReplica('leitura', key)
        if resp['data'] == None:
            reply.message = 'Produto não existe!'
        else: 
            requestReplica('deletar', key)
            reply.message = 'Produto deletado!'

        return reply 

    
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