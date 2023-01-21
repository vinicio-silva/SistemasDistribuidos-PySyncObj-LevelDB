from concurrent import futures
import json
import random
import grpc
import client_pb2
import client_pb2_grpc
import sys
from cache import Cache
import socket

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
class ClientServicer(client_pb2_grpc.ClientServicer):  
    def criarPedido(self, request_iterator, context):        
        print("Criar Pedido")
        reply = client_pb2.criarPedidoReply()
        key = str('clientId:' + request_iterator.clientId)        
        resp = requestReplica('leitura', key)
        if resp['data'] == None:
            reply.message = 'Cliente não existe!'         
        else:             
            ordemId = random.randint(0,100)
            resp = requestReplica('leitura', str(ordemId))
            while resp['data'] != None:
                ordemId = random.randint(0,100)
                resp = requestReplica('leitura', str(ordemId))
            key = str('ordemId:' + str(ordemId))
            value = {'clientId':  str(request_iterator.clientId), 'produto': '', 'quantidade': '0', 'total': '0'}
            requestReplica('inserir', key, value)
            reply.message =  f'Sua ordem do pedido é:{ordemId}'

        return reply
    # def modificarPedido(self, request_iterator, context):
    #     reply = client_pb2.modificarPedidoReply()
    #     db1,db2,db3 = startDatabase()
    #     if getData(db1, str('clientId:' + request_iterator.clientId)) == None:
    #         reply.message = 'Cliente não existe!'
    #     elif getData(db1, str('ordemId:' + request_iterator.ordemId)) == None:
    #         reply.message = 'Ordem de pedido não existe!'        
    #     else:
    #         dadosPedido = json.loads(getData(db1, str('ordemId:' + request_iterator.ordemId)))
    #         if getData(db1, str('clientId:' + request_iterator.clientId)):     
    #             for key,value in db1:
    #                 chave = key.decode().split(':')
    #                 if chave[0] == 'produtoId':
    #                     produto = json.loads(value.decode())
    #                     if produto['nome'] == request_iterator.produto:
    #                         if request_iterator.quantidade == 0:
    #                             dadosProduto = {"nome": produto['nome'], "quantidade": produto['quantidade'], "preco": produto['preco']}
    #                             insertData(db1,str('produtoId:' + key.decode()), json.dumps(dadosProduto))
    #                             dadosPedido = {'clientId':  str(request_iterator.clientId), 'produto': '','quantidade': '0', 'total': '0'}
    #                             insertData(db1, str('ordemId:' + request_iterator.ordemId), json.dumps(dadosPedido))
    #                             res = getData(db1, str('ordemId:' + request_iterator.ordemId))
    #                             res2 = getData(db1, str('produtoId:' + key.decode()))
    #                             print("Modificação realizada no pedido: " + res)
    #                             print("Modificação realizada no produto: " + res2)
    #                             reply.message = 'Pedido modificado!'
    #                             break
    #                         elif request_iterator.quantidade < 0:
    #                             reply.message = 'Digite uma quantidade maior ou igual a 0!'
    #                             break                                
    #                         elif int(produto['quantidade']) < request_iterator.quantidade:  
    #                             reply.message = 'Quantidade do produto insuficiente!'
    #                             break
    #                         else:
    #                             if request_iterator.quantidade < int(dadosPedido['quantidade']):                     
    #                                 dadosProduto = {"nome": produto['nome'], "quantidade": produto['quantidade'] - request_iterator.quantidade, "preco": produto['preco']} 
    #                                 insertData(db1,str('produtoId:' + key.decode()), json.dumps(dadosProduto))
    #                                 dadosPedido = {'clientId':  str(request_iterator.clientId), 'produto': produto['nome'],'quantidade': request_iterator.quantidade, 'total': str(produto['preco']*request_iterator.quantidade)}
    #                                 insertData(db1, str('ordemId:' + request_iterator.ordemId), json.dumps(dadosPedido))
    #                                 res = getData(db1, str('ordemId:' + request_iterator.ordemId))
    #                                 res2 = getData(db1, str('produtoId:' + key.decode()))
    #                                 print("Modificação realizada no pedido: " + res)
    #                                 print("Modificação realizada no produto: " + res2)
    #                             elif request_iterator.quantidade > int(dadosPedido['quantidade']):                                  
    #                                 dadosProduto = {"nome": produto['nome'], "quantidade": produto['quantidade'] - request_iterator.quantidade, "preco": produto['preco']}
    #                                 insertData(db1,str('produtoId:' + key.decode()), json.dumps(dadosProduto))
    #                                 dadosPedido = {'clientId':  str(request_iterator.clientId), 'produto': produto['nome'],'quantidade': request_iterator.quantidade, 'total': str(produto['preco']*request_iterator.quantidade)}
    #                                 insertData(db1, str('ordemId:' + request_iterator.ordemId), json.dumps(dadosPedido))
    #                                 res = getData(db1, str('ordemId:' + request_iterator.ordemId))
    #                                 res2 = getData(db1, str('produtoId:' + key.decode()))
    #                                 print("Modificação realizada no pedido: " + res)
    #                                 print("Modificação realizada no produto: " + res2)
    #                                 reply.message = 'Pedido modificado!'
    #                                 break
    #                             else: 
    #                                 reply.message = "Mesma quantidade do pedido"
    #                                 break

                               
    #                 else:
    #                     continue
    #             else:
    #                 reply.message = 'Produto não cadastrado!'                    
    #         else:
    #             reply.message = 'Esse cliente não tem acesso a esse pedido!'

    #     return reply
    def listarPedido(self, request_iterator, context):
        print("Listar Pedido")
        reply = client_pb2.listarPedidoReply()
        key = str('clientId:' + request_iterator.clientId)
        resp = requestReplica('leitura', key)
        key2 = str('ordemId:' + request_iterator.ordemId)
        resp2 = requestReplica('leitura', key2)
        if resp['data'] == None:
            reply.message = 'Cliente não existe!'             
        elif resp2['data'] == None:
            reply.message = 'Pedido não existe!'         
        else: 
            dadosPedido = json.loads(resp2['data'])
            reply.message = f"Pedido listado:\nProduto - {dadosPedido['produto']}\nQuantidade - {dadosPedido['quantidade']}\nTotal - {dadosPedido['total']}"

        return reply
    def listarPedidos(self, request_iterator, context):
        print("Listar Pedidos")
        reply = client_pb2.listarPedidosReply()
        reply = client_pb2.listarPedidoReply()
        key = str('clientId:' + request_iterator.clientId)
        resp = requestReplica('leitura', key)
        if resp['data'] == None:
            reply.message = 'Cliente não existe!'   
        else:
            value = 'ordemId'
            resp2 = requestReplica('leituraMultipla', key, value)
            print("Pedidos Listados")
            if resp2['data'] != []:
                pedidosObject = []
                for item in resp2['data']:
                    stringAux = f"{item}\n"   
                    pedidosObject.append(stringAux)
                reply.message = ''.join(pedidosObject) 
            else: 
                reply.message = "Não foram criados pedidos com esse cliente"
        return reply    

    # def apagarPedido(self, request_iterator, context):
    #     print("Apagar Pedido")
    #     db1,db2,db3 = startDatabase()
    #     reply = client_pb2.apagarPedidoReply()

    #     if getData(db1, str('clientId:' + request_iterator.clientId)) == None:
    #         reply.message = 'Cliente não existe!'
    #     elif getData(db1, str('ordemId:' + request_iterator.ordemId)) == None:
    #         reply.message = 'Pedido não existe!'        
    #     else:
    #         dadosPedido = json.loads(getData(db1, str('ordemId:' + request_iterator.ordemId)))
    #         if dadosPedido["clientId"] == request_iterator.clientId: 
    #                     for key,value in db1:
    #                         chave = key.decode().split(':')
    #                         if chave[0] == 'produtoId':           
    #                             produto = json.loads(value.decode())        
    #                             if produto['nome'] == dadosPedido["produto"]:
    #                                 dadosProduto = {"nome": produto['nome'], "quantidade": int(produto['quantidade']) + int(dadosPedido['quantidade']), "preco": produto['preco']}
    #                                 insertData(db1,str('produtoId:' + key.decode()), json.dumps(dadosProduto))

    #                     deleteData(db1, str('ordemId:' + request_iterator.ordemId))
    #                     print("Pedido apagado")
    #                     reply.message = 'Pedido apagado!'
    #         else:
    #             reply.message = 'Esse cliente não tem acesso a esse pedido!'
        

    #     return reply

def serve():
    porta = input("Digite uma porta para abrir o servidor: ")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    client_pb2_grpc.add_ClientServicer_to_server(ClientServicer(), server)
    server.add_insecure_port(f"localhost:{porta}")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    CacheAux = Cache()
    Socket = None
    validateReplica()
    serve()