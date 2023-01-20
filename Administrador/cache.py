from time import time
TInvalidacaoSeg = 90

class Cache():

    def __init__(self):
        self.cacheHashTable = dict()
    
    def read(self, chave):
        
        if chave in self.cacheHashTable:

            elem = self.cacheHashTable[chave]
            timestamp = int(time()) - elem['TempoDeInsercao']

            if timestamp < TInvalidacaoSeg:
                return elem['Valor']

        return None
    
    def insert(self, chave, valor):

        elem = { 'TempoDeInsercao': int(time()), 'Valor': valor }
        self.cacheHashTable[chave] = elem
