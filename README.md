# SistemasDistribuidos-PySyncObj-LevelDB
Trabalho realizado na disciplina de Sistemas Distribuídos <br>
Link para referência https://paulo-coelho.github.io/ds_notes/projeto/#etapa-2-banco-de-dados-replicado
## Instalação
### Instalar pip
python -m pip install --upgrade pip
### Instalar PySyncObj
pip install pysyncobj
### Instalar LevelDB
pip install plyvel-wheels

# Execução

## Iniciar as réplicas
cd ./Banco
Terminal 1 - python database.py 1
Terminal 2 - python database.py 2
Terminal 3 - python database.py 3

## Iniciar os clientes

Na pasta Administrador:<br>
cd ./Administrador<br>
Terminal 1 - python admin_server.py <br>
Digite uma porta para iniciar o servidor do Administrador. <br>
Terminal 2 - python admin.py <br>
Digite a mesma porta para iniciar a conexão. <br>

Na pasta Cliente:<br>
cd ./Cliente<br>
Terminal 3 - python cliente_server.py <br>
Digite uma porta para iniciar o servidor do Cliente. <br>
Terminal 4 - python cliente.py <br>
Digite a mesma porta para iniciar a conexão. <br>

Isso irá iniciar os servidores e os respectivos paineis. <br>

Para realizar os testes, basta usar o menu interativo do admin e do cliente<br>

O mecanismo de comunicação entre o cliente e o cliente_server é o GRPC <br>
O mecanismo de comunicação entre o admin e o admin_server  é o GRPC <br>
A comunicação entre os servidores e as réplicas é feita via Socket <br>

### Esquema de dados no banco
Cliente tem como chave o clientId e como dados: nome e sobrenome<br>
Produto tem como chave o productId e como dados: nome, quantidade e preço<br>
Pedido tem como chave a ordemId e como dados: clientId, produtoId, quantidade e total<br>