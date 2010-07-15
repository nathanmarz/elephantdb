(ns elephantdb.client
  (:use [clojure.contrib.seq-utils :only [shuffle includes?]])
  (:use [elephantdb thrift hadoop config types util])
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
    [[] {:local-hostname (local-hostname)
         :local-elephant local-elephant
         :global-conf global-conf
         :domain-shard-indexes (shard/shard-domains (filesystem fs-conf) global-conf)}]))

(defn- get-index [this domain]
  (let [index ((:domain-shard-indexes (. this state)) domain)]
    (when-not index (throw (domain-not-found-ex domain)))
    index ))

(defn- my-local-hostname [this]
  (:local-hostname (. this state)))

(defn get-priority-hosts [this domain key]
  (let [hosts (shuffle (shard/key-hosts (get-index this domain key)))
        localhost (my-local-hostname this)]
    (if (includes? hosts localhost)
      (cons localhost (remove-val localhost hosts))
      hosts )))

(defn- ring-port [this]
  (:port (:global-conf (. this state))))

(defn- try-get [this domain key totry]
  ;; TODO
  )

(defn -get [this domain key]
  (let [hosts (get-priority-hosts this domain key)]
    (loop [[totry & resthosts] hosts]
      (when-not totry
        (throw (hosts-down-ex hosts)))
      (if-let [ret (try-get this domain key totry)]
        ret
        (recur resthosts))
      )))

(defn -getInt [this domain i]
  (.get this domain (serialize-int i)))

(defn -getString [this domain s]
  (.get this domain (serialize-string s)))

(defn -getLong [this domain l]
  (.get this domain (serialize-long l)))
