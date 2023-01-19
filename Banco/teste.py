import plyvel
from pysyncobj import SyncObj, replicated

class MyCounter(SyncObj):
	def __init__(self):
		super(MyCounter, self).__init__('serverA:4321', ['serverB:4321', 'serverC:4321'])
		self.__counter = 0

	@replicated
	def incCounter(self):
		self.__counter += 1

	def getCounter(self):
		return self.__counter

def createDatabase():
    db = plyvel.DB('./bancos/server1/', create_if_missing=True)
    chaveBytes = bytes('1', 'utf-8')
    valorBytes = bytes('Banana','utf-8')
    db.put(chaveBytes, valorBytes)

    chaveBytes = bytes('1', 'utf-8')
    respBytes = db.get(chaveBytes)
    resp = None if not respBytes else respBytes.decode()
    print(resp)
def run():
    createDatabase()
    print('teste')
if __name__ == "__main__":
    run()