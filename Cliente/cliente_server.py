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
    requestMsg = json.dumps({'function': function, 'key': key, 'value': value})
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
        print('Cache: deletado', key)
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
            requestReplica('inserir', key, json.dumps(value))
            reply.message =  f'Sua ordem do pedido é:{ordemId}'

        return reply

    def modificarPedido(self, request_iterator, context):
        print("Modificar Pedido")
        reply = client_pb2.modificarPedidoReply()
        key = str('clientId:' + request_iterator.clientId)
        resp = requestReplica('leitura', key)
        key2 = str('ordemId:' + request_iterator.ordemId)
        resp2 = requestReplica('leitura', key2)
        key3 = str('produtoId:' + str(request_iterator.produto))
        resp3 = requestReplica('leitura', key3)
        if resp['data'] == None:
            reply.message = 'Cliente não existe!'
        elif resp2['data'] == None:
            reply.message = 'Pedido não existe!'        
        elif resp3['data'] == None:
            reply.message = 'Produto não existe!'
        else:
            dadosPedido = resp2['data']
            dadosPedido2 = json.loads(dadosPedido)
            dadosProduto = resp3['data']
            dadosProduto2 = json.loads(dadosProduto)
            if dadosPedido2["clientId"] != request_iterator.clientId:
                reply.message = 'Esse cliente não tem acesso a esse pedido!'
            else:
                if request_iterator.quantidade == 0:
                    requestReplica('deletar', key3)
                    value = {"nome": dadosProduto2['nome'], "quantidade":str(int(dadosProduto['quantidade']) + int(dadosPedido['quantidade'])), "preco": dadosProduto2['preco']}
                    requestReplica('inserir', key3, json.dumps(value))
                    print("Produto atualizado!")

                    requestReplica('deletar', key2)
                    value = {'clientId':  str(request_iterator.clientId), 'produto': str(request_iterator.produto), 'quantidade': request_iterator.quantidade, 'total': str(int(request_iterator.quantidade) * int(dadosProduto2['preco'])) }
                    requestReplica('inserir', key2, json.dumps(value))                   
                    reply.message = 'Pedido modificado!'
                
                elif request_iterator.quantidade >= int(dadosProduto2['quantidade']):
                    requestReplica('deletar', key3)
                    text = requestReplica('leitura', key3)
                    print(text)
                    value = {"nome": dadosProduto2['nome'], "quantidade":'', "preco": dadosProduto2['preco']}
                    requestReplica('inserir', key3, json.dumps(value))
                    print("Produto atualizado!")

                    requestReplica('deletar', key2)
                    value = {'clientId':  str(request_iterator.clientId), 'produto': str(request_iterator.produto), 'quantidade': dadosProduto2['quantidade'], 'total': str(int(dadosProduto2['quantidade']) * int(dadosProduto2['preco'])) }
                    requestReplica('inserir', key2, json.dumps(value))                   
                    reply.message = 'Pedido modificado!'

                elif request_iterator.quantidade < 0:
                    reply.message = "Insira uma quantidade maior que 0!"    
                else:
                    requestReplica('deletar', key3)
                    if request_iterator.quantidade > int(dadosPedido2['quantidade']):
                        value = {"nome": dadosProduto2['nome'], "quantidade":str(int(dadosProduto2['quantidade'] - (request_iterator.quantidade - int(dadosPedido2['quantidade'])))), "preco": dadosProduto2['preco']}
                        requestReplica('inserir', key3, json.dumps(value))
                        print("Produto atualizado!")
                    else:
                        value = {"nome": dadosProduto2['nome'], "quantidade":str(int(dadosProduto2['quantidade'] + (int(dadosPedido2['quantidade']) - request_iterator.quantidade))), "preco": dadosProduto2['preco']}
                        requestReplica('inserir', key3, json.dumps(value))
                        print("Produto atualizado!")

                    requestReplica('deletar', key2)
                    value = {'clientId':  str(request_iterator.clientId), 'produto': str(request_iterator.produto), 'quantidade': request_iterator.quantidade, 'total': str(request_iterator.quantidade * int(dadosProduto2['preco'])) }
                    requestReplica('inserir', key2, json.dumps(value))                   
                    reply.message = 'Pedido modificado!'         
        return reply

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
            print(resp2['data'])
            dadosPedido = json.loads(resp2['data'])
            reply.message = f"Pedido listado:\nProdutoId - {dadosPedido['produto']}\nQuantidade - {dadosPedido['quantidade']}\nTotal - {dadosPedido['total']}"

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

    def apagarPedido(self, request_iterator, context):
        print("Apagar Pedido")
        reply = client_pb2.apagarPedidoReply()
        key = str('clientId:' + request_iterator.clientId)
        resp = requestReplica('leitura', key)
        key2 = str('ordemId:' + request_iterator.ordemId)
        resp2 = requestReplica('leitura', key2)
        if resp['data'] == None:
            reply.message = 'Cliente não existe!'
        elif resp2['data'] == None:
            reply.message = 'Pedido não existe!'        
        else:
            dadosPedido = (resp2['data'])
            dadosPedido = json.loads(dadosPedido)
            key3 = str('produtoId:' + str(dadosPedido['produto']))
            resp3 = requestReplica('leitura', key3)
            dadosProduto = json.loads(resp3['data'])
            if dadosPedido["clientId"] == request_iterator.clientId:
                requestReplica('deletar', key2)
                if resp3['data'] != None:
                   requestReplica('deletar', key3)
                   value = {"nome": dadosProduto['nome'], "quantidade": str(int(dadosProduto['quantidade']) + int(dadosPedido['quantidade'])), "preco": dadosProduto['preco']}
                   requestReplica('inserir', key3, json.dumps(value))
                print("Pedido apagado")
                reply.message = 'Pedido apagado!'
            else:
               reply.message = 'Esse cliente não tem acesso a esse pedido!'
        
        return reply

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