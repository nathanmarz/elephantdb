(ns elephantdb.keyval.thrift
  (:import [org.apache.thrift.protocol TBinaryProtocol]
           [org.apache.thrift.transport TTransport TFramedTransport TSocket]
           [elephantdb.generated ElephantDB$Client]))

(defn elephant-client-and-conn [host port]
  (let [transport (TFramedTransport. (TSocket. host port))
        prot (TBinaryProtocol. transport)
        client (ElephantDB$Client. prot)]
    (.open transport)
    [client transport]))

(defmacro with-elephant-connection [host port client-sym & body]
  `(let [[^ElephantDB$Client ~client-sym ^TTransport conn#]
         (elephant-client-and-conn ~host ~port)]
     (try ~@body
          (finally (.close conn#)))))
