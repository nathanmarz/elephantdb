from genpy.elephantdb import ElephantDB

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.Thrift import TException
from thrift.transport.TTransport import TTransportException
from thrift.TSerialization import serialize, deserialize

class ElephantDBClient:
    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._reset()
        self._connect()
    
    def get(domain, key):
        return self._exec(lambda client: client.get(domain, key))
    
    def getString(domain, key):
        return self._exec(lambda client: client.getString(domain, key))
    
    def getInt(domain, key):
        return self._exec(lambda client: client.getInt(domain, key))
    
    def getLong(domain, key):
        return self._exec(lambda client: client.getLong(domain, key))
    
    def getThrift(domain, key):
        return self._exec(lambda client: client.get(domain, serialize(key)))
    
    def directGet(domain, key):
        return self._exec(lambda client: client.directGet(domain, key))
    
    def getDomainStatus(domain):
        return self._exec(lambda client: client.getDomainStatus(domain))
    
    def getDomains():
        return self._exec(lambda client: client.getDomains())
    
    def getStatus():
        return self._exec(lambda client: client.getStatus())
    
    def isFullyLoaded():
        return self._exec(lambda client: client.isFullyLoaded())
    
    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._reset()
    
    def _reset(self):
        self._conn = None
        self._client = None
    
    def _exec(self, func, trynum=1):
        self._connect()
        try:
            func(self._client)
        except (TException, TTransportException), e:
            if trynum >= 5:
                raise e
            else:
                self._reset()
                self._exec(func, trynum+1)
    
    def _connect(self):
        if self._conn is None:
            self._conn = TTransport.TFramedTransport(TSocket.TSocket(self._host, self._port))
            self._client = Collector.Client(TBinaryProtocol.TBinaryProtocol(self._conn))
            self._conn.open()
