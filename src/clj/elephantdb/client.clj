(ns elephantdb.client
  (:use [elephantdb hadoop config types])
  (:require [elephantdb [shard :as shard]])
  (:import [elephantdb.generated ElephantDB$Iface])
  (:gen-class
     :init init
     :implements [elephantdb.client.IElephantClient]
     :constructors {[elephantdb.generated.ElephantDB$Iface java.util.Map java.util.Map] []
                    [java.util.Map String] []}
     :state state ))

(defn -init
  ([fs-conf global-conf-path]
    (-init nil fs-conf (read-clj-config (filesystem fs-conf) global-conf-path)))
  ([local-elephant fs-conf global-conf]
    [[] {:local-elephant local-elephant :domain-shard-indexes (shard/shard-domains (filesystem fs-conf) global-conf)}]))

(defn- get-index [this domain]
  (let [index ((:domain-shard-indexes (. this state)) domain)]
    (when-not index (throw (IllegalArgumentException. (str domain " is not a valid domain"))))
    index ))

(defn -get [this domain key]
  (let [hosts (shard/key-hosts (get-index this domain key))]

    ))

(defn -getInt [this domain i]
  (.get this domain (serialize-int i)))

(defn -getString [this domain s]
  (.get this domain (serialize-string s)))

(defn -getLong [this domain l]
  (.get this domain (serialize-long l)))

