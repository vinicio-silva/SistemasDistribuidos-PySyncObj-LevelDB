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

def requestReplica (function, key, value=None):
    global Cache, Socket
    if (function == 'leitura'):
        cache = Cache().read(key)
        if cache != None:
            return {'msg': cache}
    requestMsg = json.dumps({'function': function, 'key': key, 'value': json.dumps(value)})
    Socket = socket.socket()
    Socket.send(requestMsg.encode())
    resp = Socket.recv(16480)
    response = json.loads(resp.decode())

    if response['msg'] == 'Operação realizada':
        if function == 'inserir':
            Cache.insert(key, value)
        if function == 'leitura':
            Cache.insert(key, response['msg'])
        if function == 'deletar':
            Cache.insert(key, None)

    return response

def validateReplica():
    global NumReplica, Socket
    inativas = 1
    while True:
        if inativas >= 3:
            print('Replicas inativas, fechando o programa')
            sys.exit()
        
        # Tenta acessar as réplicas circularmente [0,2]
        NumReplica = (NumReplica+1)%3 # 0,1 -> 1,2 -> 2,1,0
        deuExcept = False
        try:

            # tenta conectar a nova replica
            Socket = socket.socket()
            host = socket.gethostname()
            if NumReplica == 0:
                Socket.connect((host, 1050))
            elif NumReplica == 1:
                Socket.connect((host, 1060))
            else:
                Socket.connect((host, 1070))
            print('Replica ' + str(NumReplica) + ' escolhida!')

        except BaseException as e:
            deuExcept = True

        if not deuExcept:
            break

        inativas = inativas+1
class AdminServicer(admin_pb2_grpc.AdminServicer):  
    def inserirCliente(self, request_iterator, context):        
        print("Inserir Cliente")
        reply = admin_pb2.inserirClienteReply()
        key = str('clientId:' + request_iterator.clientId)
        value = request_iterator.dadosCliente
        resp = requestReplica('leitura', key)
        if resp != None:
             reply.message = 'Cliente já existe!'         
        else: 
            resp2 = requestReplica('inserir', key, value)
            if resp2['msg'] == 'Operação realizada':
                print("Operação realizada")
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
    # def recuperarCliente(self, request_iterator, context):
    #     db1,db2,db3 = startDatabase()
    #     print("Recuperar Cliente")

    #     reply = admin_pb2.recuperarClienteReply()

    #     resp = getData(db1, str('clientId:' + request_iterator.clientId))

    #     if resp == None:
    #         reply.message = 'Cliente não existe!'         
    #     else: 
    #         dadosCliente = json.loads(resp)
    #         reply.message = f"Cliente recuperado:\nNome - {dadosCliente['nome']}\nSobrenome - {dadosCliente['sobrenome']}"

    #     return reply
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
    Socket = None
    NumReplica = 0
    validateReplica()
    serve()