from concurrent import futures
import ast
import json
import random
import grpc
import client_pb2
import client_pb2_grpc
from paho.mqtt import client as mqtt
import plyvel

mqttBroker = "mqtt.eclipseprojects.io"
client = mqtt.Client("Client Server")
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


class ClientServicer(client_pb2_grpc.ClientServicer):  
    def criarPedido(self, request_iterator, context):
        db1,db2,db3 = startDatabase()
        print("Criar Pedido")
        reply = client_pb2.criarPedidoReply()
        if getData(db1, str('clientId:' + request_iterator.clientId)):          
            ordemId = random.randint(0,100)
            while getData(db1, str('ordemId:' + str(ordemId))):
                ordemId = random.randint(0,100)
            dadosPedido = {'clientId':  str(request_iterator.clientId), 'produto': '', 'quantidade': '0', 'total': '0'}
            insertData(db1,str('ordemId:' + str(ordemId)), json.dumps(dadosPedido)) 
            resp = getData(db1, str('ordemId:' + str(ordemId)))
            print("Criação realizada: " + resp)
            reply.message = f'Sua ordem do pedido é:{ordemId}'        
        else: 
            reply.message = 'Cliente não existe!'

        return reply
    def modificarPedido(self, request_iterator, context):
        reply = client_pb2.modificarPedidoReply()
        db1,db2,db3 = startDatabase()
        if getData(db1, str('clientId:' + request_iterator.clientId)) == None:
            reply.message = 'Cliente não existe!'
        elif getData(db1, str('ordemId:' + request_iterator.ordemId)) == None:
            reply.message = 'Ordem de pedido não existe!'        
        else:
            dadosPedido = json.loads(getData(db1, str('ordemId:' + request_iterator.ordemId)))
            if getData(db1, str('clientId:' + request_iterator.clientId)):     
                for key,value in db1:
                    chave = key.decode().split(':')
                    if chave[0] == 'produtoId':
                        produto = json.loads(value.decode())
                        if produto['nome'] == request_iterator.produto:
                            if request_iterator.quantidade == 0:
                                dadosProduto = {"nome": produto['nome'], "quantidade": produto['quantidade'], "preco": produto['preco']}
                                insertData(db1,str('produtoId:' + key.decode()), json.dumps(dadosProduto))
                                dadosPedido = {'clientId':  str(request_iterator.clientId), 'produto': '','quantidade': '0', 'total': '0'}
                                insertData(db1, str('ordemId:' + request_iterator.ordemId), json.dumps(dadosPedido))
                                res = getData(db1, str('ordemId:' + request_iterator.ordemId))
                                res2 = getData(db1, str('produtoId:' + key.decode()))
                                print("Modificação realizada no pedido: " + res)
                                print("Modificação realizada no produto: " + res2)
                                reply.message = 'Pedido modificado!'
                                break
                            elif request_iterator.quantidade < 0:
                                reply.message = 'Digite uma quantidade maior ou igual a 0!'
                                break                                
                            elif int(produto['quantidade']) < request_iterator.quantidade:  
                                reply.message = 'Quantidade do produto insuficiente!'
                                break
                            else:
                                if request_iterator.quantidade < int(dadosPedido['quantidade']):                     
                                    dadosProduto = {"nome": produto['nome'], "quantidade": produto['quantidade'] - request_iterator.quantidade, "preco": produto['preco']} 
                                    insertData(db1,str('produtoId:' + key.decode()), json.dumps(dadosProduto))
                                    dadosPedido = {'clientId':  str(request_iterator.clientId), 'produto': produto['nome'],'quantidade': request_iterator.quantidade, 'total': str(produto['preco']*request_iterator.quantidade)}
                                    insertData(db1, str('ordemId:' + request_iterator.ordemId), json.dumps(dadosPedido))
                                    res = getData(db1, str('ordemId:' + request_iterator.ordemId))
                                    res2 = getData(db1, str('produtoId:' + key.decode()))
                                    print("Modificação realizada no pedido: " + res)
                                    print("Modificação realizada no produto: " + res2)
                                elif request_iterator.quantidade > int(dadosPedido['quantidade']):                                  
                                    dadosProduto = {"nome": produto['nome'], "quantidade": produto['quantidade'] - request_iterator.quantidade, "preco": produto['preco']}
                                    insertData(db1,str('produtoId:' + key.decode()), json.dumps(dadosProduto))
                                    dadosPedido = {'clientId':  str(request_iterator.clientId), 'produto': produto['nome'],'quantidade': request_iterator.quantidade, 'total': str(produto['preco']*request_iterator.quantidade)}
                                    insertData(db1, str('ordemId:' + request_iterator.ordemId), json.dumps(dadosPedido))
                                    res = getData(db1, str('ordemId:' + request_iterator.ordemId))
                                    res2 = getData(db1, str('produtoId:' + key.decode()))
                                    print("Modificação realizada no pedido: " + res)
                                    print("Modificação realizada no produto: " + res2)
                                    reply.message = 'Pedido modificado!'
                                    break
                                else: 
                                    reply.message = "Mesma quantidade do pedido"
                                    break

                               
                    else:
                        continue
                else:
                    reply.message = 'Produto não cadastrado!'                    
            else:
                reply.message = 'Esse cliente não tem acesso a esse pedido!'

        return reply
    def listarPedido(self, request_iterator, context):
        global dicionarioPedido, dicionarioClient
        print("Listar Pedido")
        reply = client_pb2.listarPedidoReply()

        if request_iterator.clientId not in dicionarioClient:
            reply.message = 'Cliente não existe!'
        elif request_iterator.ordemId not in dicionarioPedido:
            reply.message = 'Pedido não existe!'        
        else:
            dadosPedido = json.loads(dicionarioPedido[request_iterator.ordemId])
            if dadosPedido['clientId'] == request_iterator.clientId:
                reply.message = f"Pedido listado:\nProduto - {dadosPedido['produto']}\nQuantidade - {dadosPedido['quantidade']}\nTotal - {dadosPedido['total']}"
            else:
                reply.message = 'Esse cliente não tem acesso a esse pedido!'

        return reply
    def listarPedidos(self, request_iterator, context):
        global dicionarioPedido, dicionarioClient
        print("Listar Pedidos")
        reply = client_pb2.listarPedidosReply()
        
        if request_iterator.clientId not in dicionarioClient:
            reply.message = 'Cliente não existe!'
        else:
            pedidosObject = []
            for ordemId in dicionarioPedido:
                dadosPedido = ast.literal_eval(dicionarioPedido[ordemId])            
                if dadosPedido['clientId'] == request_iterator.clientId:
                    stringAux = f"Pedidos criados: OrdemId - {ordemId} | Total - {dadosPedido['total']}\n"
                    pedidosObject.append(stringAux)
            if pedidosObject != []:
                reply.message = ''.join(pedidosObject)
            else:
                reply.message = "Não foram criados nenhum pedido com esse clientId"     
        print("Pedidos Listados: " + str(dicionarioPedido))   
        return reply

    def apagarPedido(self, request_iterator, context):
        print("Apagar Pedido")

        reply = client_pb2.apagarPedidoReply()

        if request_iterator.clientId not in dicionarioClient:
            reply.message = 'Cliente não existe!'
        elif request_iterator.ordemId not in dicionarioPedido:
            reply.message = 'Pedido não existe!'        
        else:
            dadosPedido = json.loads(dicionarioPedido[request_iterator.ordemId])
            if dadosPedido['clientId'] == request_iterator.clientId:
                for produtoId in dicionarioProduct:            
                        produto = ast.literal_eval(dicionarioProduct[produtoId])         
                        if produto['nome'] == dadosPedido['produto']:
                            dadosProduto = {"nome": produto['nome'], "quantidade": str(int(produto['quantidade']) + int(dadosPedido['quantidade'])), "preco": produto['preco']}
                            dicionarioProduct[produtoId] = json.dumps(dadosProduto)

                dicionarioPedido.pop(request_iterator.ordemId)
                client.publish("ApagarPedido", str(dicionarioProduct))
                print("Pedido apagado: " + str(dicionarioPedido))
                reply.message = 'Pedido apagado!'
            else:
                reply.message = 'Esse cliente não tem acesso a esse pedido!'
        

        return reply

    def on_message(client, userdata, message):
        msg = message.payload.decode("utf-8").split('/')
        print("Cadastros existentes: ", msg[0])
        global dicionarioClient
        dicionarioClient = ast.literal_eval(msg[0])
        print("Produtos cadastrados: ", msg[1])
        global dicionarioProduct
        dicionarioProduct = ast.literal_eval(msg[1])

    client.loop_start()
    client.subscribe("InserirCliente")
    client.subscribe("ModificarCliente")
    client.subscribe("ApagarCliente")
    client.subscribe("InserirProduto")
    client.subscribe("ModificarProduto")
    client.subscribe("ApagarProduto")
    client.on_message = on_message

def serve():
    porta = input("Digite uma porta para abrir o servidor: ")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    client_pb2_grpc.add_ClientServicer_to_server(ClientServicer(), server)
    server.add_insecure_port(f"localhost:{porta}")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()