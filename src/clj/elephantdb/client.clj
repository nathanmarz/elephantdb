(ns elephantdb.client
  (:use [clojure.contrib.seq-utils :only [includes?]])
  (:use [elephantdb thrift hadoop config types util log])
  (:require [elephantdb [shard :as shard]])
  (:require [clojure.contrib.seq-utils :as seq-utils])
  (:import [elephantdb.generated ElephantDB$Iface WrongHostException DomainNotFoundException DomainNotLoadedException])
  (:import [org.apache.thrift TException])
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

(defn- my-local-elephant [this]
  (:local-elephant (. this state)))

(defn- my-local-hostname [this]
  (:local-hostname (. this state)))

(defn- get-priority-hosts [this domain key]
  (let [hosts (shuffle (shard/key-hosts domain (get-index this domain) key))
        localhost (my-local-hostname this)]
    (if (includes? hosts localhost)
      (cons localhost (remove-val localhost hosts))
      hosts )))

(defn- ring-port [this]
  (:port (:global-conf (. this state))))

(defn multi-get-remote [host port domain keys]
  (with-elephant-connection host port client
    (.directMultiGet client domain keys)))

(defn- try-multi-get [this domain keys totry]
  (try
    (if (and (my-local-elephant this) (= totry (my-local-hostname this)))
      (.directMultiGet (my-local-elephant this) domain keys)
      (multi-get-remote totry (ring-port this) domain keys))
  (catch TException e
    ;; try next host
    (log-error e "Thrift exception on " totry ":" domain "/" keys)
    nil )
  (catch WrongHostException e
    (log-error e "Fatal exception on " totry ":" domain "/" keys)
    (throw (TException. "Fatal exception when performing get" e)))
  (catch DomainNotFoundException e
    (log-error e "Could not find domain when executing read on " totry ":" domain "/" keys)
    (throw e))
  (catch DomainNotLoadedException e
    (log-error e "Domain not loaded when executing read on " totry ":" domain "/" keys)
    (throw e))
  ))

(defn -get [this domain key]
  (first
   (.multiGet this domain [key])))

(defn -getInt [this domain i]
  (.get this domain (serialize-int i)))

(defn -getString [this domain s]
  (.get this domain (serialize-string s)))

(defn -getLong [this domain l]
  (.get this domain (serialize-long l)))

;; returns [hosts-to-try global-index key all-hosts] seq
(defn- host-indexed-keys [this domain keys]
  (let [indexed (seq-utils/indexed keys)]
    (for [[gi key] indexed]
      (let [priority-hosts (get-priority-hosts this domain key)]
        [priority-hosts gi key priority-hosts]
        ))))

;; executes multi-get, returns seq of [global-index val]
(defn- multi-get* [this domain host host-indexed-keys key-shard-fn multi-get-remote-fn]
  (binding [shard/key-shard key-shard-fn
            multi-get-remote multi-get-remote-fn]
    (if-let [vals (try-multi-get this domain (map third host-indexed-keys) host)]
      (map (fn [v [hosts gi key all-hosts]] [gi v])
           vals host-indexed-keys
           ))))

(defn -multiGet [this domain keys]
  (let [;; this trickery is to get around issues with binding/tests
        key-shard-fn shard/key-shard
        multi-get-remote-fn multi-get-remote
        host-indexed-keys (host-indexed-keys this domain keys)]
    (loop [keys-to-get host-indexed-keys
           results []]
      (let [host-map (group-by #(first (first %)) keys-to-get)
            rets (parallel-exec
                  (for [[host host-indexed-keys] host-map]
                    (fn []
                      [host
                       (multi-get* this domain host host-indexed-keys key-shard-fn multi-get-remote-fn)]
                      )))
            succeeded (filter second rets)
            succeeded-hosts (map first succeeded)
            results (apply concat results (map second succeeded))
            failed-host-map (apply dissoc host-map succeeded-hosts)
            ]
        (if-not (empty? failed-host-map)
          (recur
           (for [[[_ & hosts] gi key all-hosts] (apply concat (vals failed-host-map))]
             (do
               (when (empty? hosts)
                 (throw (hosts-down-ex all-hosts)))
               [hosts gi key all-hosts]))
           results )
          (map second (sort-by first results))
          )))
    ))

(defn -multiGetInt [this domain integers]
  (.multiGet this domain (map serialize-int integers)))

(defn -multiGetString [this domain strings]
  (.multiGet this domain (map serialize-string strings)))

(defn -multiGetLong [this domain longs]
  (.multiGet this domain (map serialize-long longs)))
