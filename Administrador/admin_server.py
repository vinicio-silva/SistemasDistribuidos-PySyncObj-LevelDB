import ast
from concurrent import futures
import grpc
import admin_pb2
import admin_pb2_grpc
from paho.mqtt import client as mqtt
import json
import plyvel

mqttBroker = "mqtt.eclipseprojects.io"
client = mqtt.Client("Admin Server")
client.connect(mqttBroker)

def startDatabase():
    db1 = plyvel.DB('../Banco/bancos/banco1/', create_if_missing=True)
    db2 = plyvel.DB('../Banco/bancos/banco2/', create_if_missing=True)
    db3 = plyvel.DB('../Banco/bancos/banco3/', create_if_missing=True)
    return db1,db2,db3

def insertData(db, chave, valor):
    chaveBytes = bytes(chave, 'utf-8')
    valorBytes = bytes(valor,'utf-8')
    db.put(chaveBytes, valorBytes)

def getData(db, chave):
    chaveBytes = bytes(chave, 'utf-8')
    respBytes = db.get(chaveBytes)
    resp = None if not respBytes else respBytes.decode()
    return resp

def deleteData(db, chave):
    chaveBytes = bytes(chave, 'utf-8')
    db.delete(chaveBytes)
class AdminServicer(admin_pb2_grpc.AdminServicer):  
    def inserirCliente(self, request_iterator, context):
        db1,db2,db3 = startDatabase()
        print("Inserir Cliente")
        reply = admin_pb2.inserirClienteReply()
        if getData(db1, str('clientId:' + request_iterator.clientId)):
            reply.message = 'Cliente já existe!'         
        else: 
            insertData(db1,str('clientId:' + request_iterator.clientId), request_iterator.dadosCliente)            
            resp = getData(db1, str('clientId:' + request_iterator.clientId))
            print("Inserção realizada:" + resp)
            reply.message = 'Cliente inserido!'

        return reply
    def modificarCliente(self, request_iterator, context):
        db1,db2,db3 = startDatabase()
        print("Modificar Cliente")      
        reply = admin_pb2.modificarClienteReply()        
        if getData(db1, str('clientId:' + request_iterator.clientId)) == None:
            reply.message = 'Cliente não existe!'         
        else: 
            novosDados = json.loads(request_iterator.dadosCliente)
            dadosCliente = {"nome": novosDados['nome'], "sobrenome": novosDados['sobrenome']}
            insertData(db1,str('clientId:' + request_iterator.clientId), json.dumps(dadosCliente))
            resp = getData(db1, str('clientId:' + request_iterator.clientId))
            print("Modificação realizada: " + resp)
            reply.message = 'Cliente modificado!'

        return reply
    def recuperarCliente(self, request_iterator, context):
        db1,db2,db3 = startDatabase()
        print("Recuperar Cliente")

        reply = admin_pb2.recuperarClienteReply()

        resp = getData(db1, str('clientId:' + request_iterator.clientId))

        if resp == None:
            reply.message = 'Cliente não existe!'         
        else: 
            dadosCliente = json.loads(resp)
            reply.message = f"Cliente recuperado:\nNome - {dadosCliente['nome']}\nSobrenome - {dadosCliente['sobrenome']}"

        return reply
    def apagarCliente(self, request_iterator, context):
        db1,db2,db3 = startDatabase()
        print("Apagar Cliente")

        reply = admin_pb2.apagarClienteReply()
        
        if getData(db1, str('clientId:' + request_iterator.clientId)) == None:
            reply.message = 'Cliente não existe!'         
        else: 
            deleteData(db1, str('clientId:' + request_iterator.clientId))
            print("Cliente apagado")
            reply.message = 'Cliente apagado!'

        return reply
    def inserirProduto(self, request_iterator, context):
        db1,db2,db3 = startDatabase()
        print("Inserir Produto")
        reply = admin_pb2.inserirProdutoReply()
        if getData(db1, str('produtoId:' + request_iterator.produtoId)):
            reply.message = 'Produto já existe!'         
        else: 
            insertData(db1,str('produtoId:' + request_iterator.produtoId), request_iterator.dadosProduto)            
            resp = getData(db1, str('produtoId:' + request_iterator.produtoId))
            print("Cadastro realizada:" + resp)
            reply.message = 'Produto cadastrado!'

        return reply
    def modificarProduto(self, request_iterator, context):
        db1,db2,db3 = startDatabase()
        print("Modificar Produto")      
        reply = admin_pb2.modificarProdutoReply()
        
        if getData(db1, str('produtoId:' + request_iterator.produtoId)) == None:
            reply.message = 'Produto não existe!'         
        else: 
            novosDados = json.loads(request_iterator.dadosProduto)
            dadosProduto = {"nome": novosDados['nome'], "quantidade": novosDados['quantidade'], "preco": novosDados['preco']}
            insertData(db1,str('produtoId:' + request_iterator.produtoId), json.dumps(dadosProduto))            
            resp = getData(db1, str('produtoId:' + request_iterator.produtoId))
            print("Modificação realizada:" + resp)
            reply.message = 'Produto modificado!'

        return reply
    def recuperarProduto(self, request_iterator, context):
        db1,db2,db3 = startDatabase()
        print("Recuperar Produto")

        reply = admin_pb2.recuperarProdutoReply()
        
        if getData(db1, str('produtoId:' + request_iterator.produtoId)) == None:
            reply.message = 'Produto não existe!'         
        else: 
            resp = getData(db1, str('produtoId:' + request_iterator.produtoId))
            print("Recuperação realizada:" + resp)
            dadosProduto = json.loads(resp)
            reply.message = f"Produto recuperado:\nNome - {dadosProduto['nome']}\nQuantidade - {dadosProduto['quantidade']}\nPreço - {dadosProduto['preco']}"

        return reply
    def apagarProduto(self, request_iterator, context):
        db1,db2,db3 = startDatabase()
        print("Apagar Produto")

        reply = admin_pb2.apagarProdutoReply()
        
        if getData(db1, str('produtoId:' + request_iterator.produtoId)) == None:
            reply.message = 'Produto não existe!'         
        else: 
            deleteData(db1, str('produtoId:' + request_iterator.produtoId))
            print("Produto apagado")
            reply.message = 'Produto apagado!'

        return reply

    
def serve():
    porta = input("Digite uma porta para abrir o servidor: ")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    admin_pb2_grpc.add_AdminServicer_to_server(AdminServicer(), server)
    server.add_insecure_port(f"localhost:{porta}")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()