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

(defn- get-priority-hosts [this domain shard]
  (let [hosts (shuffle (shard/shard-hosts (get-index this domain) shard))
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


;; returns map of shard to list of [global index], key pairs
(defn- sharded-keys [this domain keys]
  (let [indexed (seq-utils/indexed keys)]
    (group-by
     (fn [[global-index key]]
       (shard/key-shard domain
                        key
                        (shard/num-shards (get-index this domain))
                        ))
     indexed
     )))


;;TODO: need to do this by host, not by shard...
;;returns list of [global-index result-Value]
(defn- shard-result-future [this domain shard indexed-keys]
  (let [key-shard-curr shard/key-shard]
    (future
     ;; Need to do this because we rebind key-shard in tests
     ;; and bindings don't cross thread bindings
     (binding [shard/key-shard key-shard-curr]
       (let [keys (map second indexed-keys)
             hosts (get-priority-hosts this domain shard)]
         (println "hosts " shard hosts)
         (loop [[totry & resthosts] hosts]
           (when-not totry
             (throw (hosts-down-ex hosts)))
           (if-let [ret (try-multi-get this domain keys totry)]
             (map (fn [[global-index _] v] [global-index v])
                  indexed-keys ret)
             (recur resthosts))
           )))
     )))

(defn -multiGet [this domain keys]
  (let [sharded-keys (sharded-keys this domain keys)
        _ (println "sharded keys: " sharded-keys)
        shard-result-futures (dofor [[shard indexed-keys] sharded-keys]
                                    (shard-result-future this domain shard indexed-keys))
        indexed-results (apply concat (future-values shard-result-futures))]
    (map second (sort-by first indexed-results))
    ))

(defn -multiGetInt [this domain integers]
  (.multiGet this domain (map serialize-int integers)))

(defn -multiGetString [this domain strings]
  (.multiGet this domain (map serialize-string strings)))

(defn -multiGetLong [this domain longs]
  (.multiGet this domain (map serialize-long longs)))
