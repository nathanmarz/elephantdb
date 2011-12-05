(ns elephantdb.keyval.thrift
  (:import [org.apache.thrift.protocol TBinaryProtocol]
           [org.apache.thrift.transport TTransport TFramedTransport TSocket]
           [elephantdb.generated ElephantDB$Client]))

(defn thrift-transport [host port]
  (TFramedTransport. (TSocket. host port)))

(defn elephant-client [transport]
  (ElephantDB$Client. (TBinaryProtocol. transport)))

(defmacro with-elephant-connection [host port client-sym & body]
  `(with-open [^TTransport conn# (thrift-transport ~host ~port)]
     (let [^ElephantDB$Client ~client-sym (elephant-client conn#)]
       ~@body)))


