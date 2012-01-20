(ns elephantdb.common.kryo
  "Namespace for inter-machine Kryo service calls."
  (:import [java.net InetAddress]
           [com.esotericsoftware.kryonet Server Client
            Listener FrameworkMessage EndPoint]
           [com.esotericsoftware.kryonet.rmi ObjectSpace]
           [elephantdb.persistence Shutdownable]))

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
