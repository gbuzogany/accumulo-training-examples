import sys
sys.path.append('proxy/thrift/gen-py')
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol
from accumulo import AccumuloProxy
from accumulo.ttypes import *

client = None
login = None

def main(args):
    global client
    global login
    
    transport = TSocket.TSocket(args[1], 42424)
    transport = TTransport.TFramedTransport(transport)
    protocol = TCompactProtocol.TCompactProtocol(transport)
    client = AccumuloProxy.Client(protocol)
    transport.open()
    login = client.login(args[2], {'password': args[3]})
    

if __name__ == '__main__':
    main(sys.argv)
