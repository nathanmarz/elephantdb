(ns elephantdb.common.kryo
  "Namespace for inter-machine Kryo service calls."
  (:require [elephantdb.common.database :as db])
  (:import [java.net InetAddress]
           [com.esotericsoftware.kryonet Server Client
            Listener FrameworkMessage EndPoint]
           [com.esotericsoftware.kryonet.rmi ObjectSpace]
           [elephantdb.persistence Shutdownable]))

(defn kryo-registrations
  [local-store]
  "TODO: Take a list of lists of kryo pairs, return the proper thrift
   business."
  (-> local-store .getSpec .getKryoPairs))

(defn kryo-get
  [service database domain-name key]
  (thrift/assert-domain database domain-name)
  (let [ser (.serializer (db/domain-get database domain-name))]
    (.kryoGet service domain-name (.serialize ser key))))

(defn get-registrations [database domain-name]
  (let [domain (db/domain-get database domain-name)]
    (kryo-registrations (.localStore domain))))

(defn prep [^EndPoint point]
  (doto (.getKryo point)
    (ObjectSpace/registerClasses)
    (.setRegistrationOptional true)))

(defn with-kryonet [kryo-fn]
  (let [server (Server.)
        client (Client.)]
    (try (.start server)
         (.bind server 54555)
         (.start client)
         (kryo-fn server client)
         (finally (.stop client)
                  (.stop server)))))

(def DATABASE-ID 42)

(defn localhost []
  (.getHostAddress (InetAddress/getLocalHost)))

(defn check-test [klass obj afn]
  (with-kryonet
    (fn [server client]
      (doall (map prep [server client]))
      (doto (.getKryo server)
        (.register klass))
      (doto (.getKryo client)
        (.register klass))
      (let [ space (ObjectSpace.)]
        (.register space DATABASE-ID obj)
        (.addListener server (proxy [Listener] []
                               (connected [conn]
                                 (.addConnection space conn))))
        (.connect client 1000 (localhost) 54555)
        (let [o (ObjectSpace/getRemoteObject client DATABASE-ID klass)]
          (print "Local: ")
          (time (dotimes [_ 1000]
                  (afn obj)))
          (print "Remote: ")
          (time (dotimes [_ 1000]
                  (afn o))))))))

(defprotocol KryoOps
  (create! [_]))

(deftype CouchDB [url]
  KryoOps
  (create! [this]
    (.connect (:client this)
              400
              (:url this) 54555)
    (ObjectSpace/getRemoteObject client DATABASE-ID klass))
  clojure.lang.ILookup
  (valAt [this k] (.get url k))
  (valAt [this k default] (or (.valAt this k) default)))
