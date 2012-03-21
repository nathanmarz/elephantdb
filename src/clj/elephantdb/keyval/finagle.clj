(ns elephantdb.keyval.finagle
  (:use [elephantdb.common.domain :only (loaded?)])
  (:require [jackknife.core :as u]
            [jackknife.logging :as log]
            [elephantdb.common.database :as db]
            [elephantdb.common.status :as status]
            [elephantdb.common.thrift :as thrift]
            [elephantdb.common.config :as conf]
            [elephantdb.keyval.domain :as dom])
  (:import [com.twitter.util Future Duration FutureEventListener]
           [java.nio ByteBuffer]
           [java.net InetSocketAddress]
           [org.apache.thrift.protocol TBinaryProtocol$Factory]
           [org.apache.thrift.transport TTransport]
           [org.apache.thrift TException]
           [elephantdb.common.database Database]
           [elephantdb.common.domain Domain]
           [elephantdb.generated DomainNotFoundException
            ElephantService$ServiceIface ElephantService$Service
            ElephantService$ServiceToClient
            DomainNotLoadedException WrongHostException]
           [elephantdb.generated.keyval ElephantDB$Client
            ElephantDB$Iface ElephantDB$Processor]
           
           [com.twitter.finagle.builder ServerBuilder ClientBuilder]
           [com.twitter.finagle.thrift
            ThriftServerFramedCodec ThriftClientFramedCodec]

           ;; Ostrich Integration
           [com.twitter.ostrich.admin.config ServerConfig
            AdminServiceConfig]
           [com.twitter.ostrich.admin RuntimeEnvironment Service
            ServiceTracker]))

;; This whole namespace is an exploration of how to go about
;; instantiating a service using finagle. The tough part seems to be
;; idiomatic integration of ostrich; I'm going to leave that for the
;; next round of coding.

(deftype ElephantServer [state-atom]
  com.twitter.ostrich.admin.Service
  (start [_])
  (shutdown [_])
  (quiesce [this] (.shutdown this))
  (reload [_])

  ;; The ElephantService is here as a demo, not anything to do with
  ;; ElephantDB proper.
  ElephantService$ServiceIface
  (get [_ key]
    (if-let [ret (get @state-atom key)]
      (Future/value "hammer")
      (Future/exception (WrongHostException.))))
  (put [_ key val]
    (swap! state-atom assoc key val)
    (Future/void)))

(def my-server
  (ElephantServer. (atom {})))

(defn ostrich-conf
  ""
  []
  (-> (AdminServiceConfig.)
      (.apply)
      (.apply (RuntimeEnvironment. nil))))

;; Documentation here:
;; http://twitter.github.com/finagle/api/finagle-core/com/twitter/finagle/builder/ServerBuilder.html

(defn kill!
  "Closes the supplied service."
  ([service]
     (.close service (Duration/MaxValue)))
  ([service timeout-ms]
     (.close service (Duration. (* 1e6 timeout-ms)))))

(defn create-server [server]
  (let [server  (ElephantService$Service. server (TBinaryProtocol$Factory.))
        builder (-> (ServerBuilder/get)
                    (.codec (ThriftServerFramedCodec/get))
                    (.name "ElephantDBServer")
                    (.bindTo (InetSocketAddress. 5378)))]
    (ServerBuilder/safeBuild server builder)))

(defn create-client []
  (let [transport (-> (ClientBuilder/get)
                      (.hosts (InetSocketAddress. 5378))
                      (.codec (ThriftClientFramedCodec/get))
                      (.hostConnectionLimit 100))]
    (ElephantService$ServiceToClient. (ClientBuilder/safeBuild transport)
                                      (TBinaryProtocol$Factory.))))

(defn run-test
  "Creates an example client and server, places a value and
  asynchronously gets it a bunch of times."
  []
  (let [server (create-server my-server)
        client (create-client)]
    (.apply (.put client "x" "fancy"))
    (try (let [futures (for [x (range 100)]
                         (-> (.get client "x")
                             (.addEventListener
                              (reify FutureEventListener
                                (onSuccess [_ x] (prn x))
                                (onFailure [_ x] (prn x))))))]
           (dorun (map #(.apply %) futures)))
         (finally (kill! server)))))
