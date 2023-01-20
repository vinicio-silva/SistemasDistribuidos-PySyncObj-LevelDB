from time import time
TInvalidacaoSeg = 90

class Cache():

    def __init__(self):
        self.cacheHashTable = dict()
    
    def read(self, key):
        
        if key in self.cacheHashTable:

            elem = self.cacheHashTable[key]
            timestamp = int(time()) - elem['TempoDeInsercao']

            if timestamp < TInvalidacaoSeg:
                return elem['Valor']

        return None
    
    def insert(self, key, value):
        
        elem = { 'TempoDeInsercao': int(time()), 'Valor': value }
        self.cacheHashTable[key] = elem
