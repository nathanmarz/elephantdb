from genpy.keyval import ElephantDB

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.Thrift import TException
from thrift.transport.TTransport import TTransportException
from thrift.TSerialization import serialize, deserialize
import socket

class ElephantDBClient:
    def __init__(self, host, port, timeout=None):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._reset()
        self._connect()

    def get(self, domain, key):
        return self._exec(lambda client: client.get(domain, key))

    def getString(self, domain, key):
        return self._exec(lambda client: client.getString(domain, key))

    def getInt(self, domain, key):
        return self._exec(lambda client: client.getInt(domain, key))

    def getLong(self, domain, key):
        return self._exec(lambda client: client.getLong(domain, key))

    def getThrift(self, domain, key):
        return self._exec(lambda client: client.get(domain, serialize(key)))

    def directGet(self, domain, key):
        return self._exec(lambda client: client.directGet(domain, key))

    def multiGet(self, domain, keys):
        return self._exec(lambda client: client.multiGet(domain, keys))

    def multiGetString(self, domain, keys):
        return self._exec(lambda client: client.multiGetString(domain, keys))

    def multiGetInt(self, domain, keys):
        return self._exec(lambda client: client.multiGetInt(domain, keys))
    
    def multiGetLong(self, domain, keys):
        return self._exec(lambda client: client.multiGetLong(domain, keys))

    def multiGetThrift(self, domain, keys):
        return self._exec(lambda client: client.multiGet(domain, map(serialize, keys)))

    def getDomainStatus(self, domain):
        return self._exec(lambda client: client.getDomainStatus(domain))

    def getDomains(self):
        return self._exec(lambda client: client.getDomains())

    def getStatus(self):
        return self._exec(lambda client: client.getStatus())

    def isFullyLoaded(self):
        return self._exec(lambda client: client.isFullyLoaded())

    def updateAll(self):
        return self._exec(lambda client: client.updateAll())

    def updateDomain(self, domain):
        return self._exec(lambda client: client.updateDomain(domain))

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
            return func(self._client)
        except (TException, TTransportException, socket.timeout, socket.error, MemoryError, EOFError), e:
            if trynum >= 5:
                raise e
            else:
                self._reset()
                return self._exec(func, trynum+1)

    def _connect(self):
        if self._conn is None:
            socket = TSocket.TSocket(self._host, self._port)
            socket.setTimeout(self._timeout)
            self._conn = TTransport.TFramedTransport(socket)
            self._client = ElephantDB.Client(TBinaryProtocol.TBinaryProtocol(self._conn))
            self._conn.open()

    def __del__(self):
        self.close()
