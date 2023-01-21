from collections import OrderedDict
from time import time


Temporizador = 120

class Cache():

    def __init__(self):
        self.cache = OrderedDict() 
    
    def read(self, key):

        if key not in self.cache:
                return None

        else:
            elem = self.cache[key]
            timestamp = int(time()) - elem['Temporizador']

            if timestamp < Temporizador:
                return elem['Valor']
            else:
                self.cache.popitem(elem)
                return None
    
    def insert(self, key, value):
        elem = { 'Temporizador': int(time()), 'Valor': value }
        self.cache[key] = elem
        self.cache.move_to_end(key)
            
