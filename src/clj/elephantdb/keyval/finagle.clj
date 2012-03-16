(ns elephantdb.keyval.finagle
  (:use [elephantdb.common.domain :only (loaded?)])
  (:require [jackknife.core :as u]
            [jackknife.logging :as log]
            [elephantdb.common.database :as db]
            [elephantdb.common.status :as status]
            [elephantdb.common.thrift :as thrift]
            [elephantdb.common.config :as conf]
            [elephantdb.keyval.domain :as dom])
  (:import [java.nio ByteBuffer]
           [org.apache.thrift.protocol TBinaryProtocol]
           [org.apache.thrift.transport TTransport]
           [org.apache.thrift TException]
           [elephantdb.common.database Database]
           [elephantdb.common.domain Domain]
           [elephantdb.generated ElephantService$Iface
            DomainNotFoundException
            DomainNotLoadedException WrongHostException]
           [elephantdb.generated.keyval ElephantDB$Client
            ElephantDB$Iface ElephantDB$Processor]

           ;; Ostrich Integration
           [com.twitter.ostrich.admin.config ServerConfig]
           [com.twitter.ostrich.admin RuntimeEnvironment]))

(deftype ElephantServer [state-atom]
  ElephantService$Iface
  (get [_ key]
    (get @state-atom key))
  (put [_ key val]
    (swap! state-atom assoc key val)))

(defn mk-config []
  (proxy [ServerConfig] []
    (apply []
      )))

(def my-server
  (ElephantServer. (atom {})))


