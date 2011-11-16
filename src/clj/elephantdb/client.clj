(ns elephantdb.client
  (:use elephantdb.config
        elephantdb.thrift
        elephantdb.common.hadoop
        elephantdb.common.util
        hadoop-util.core
        [elephantdb.common.types :only (serialize)])
  (:require [elephantdb.common.shard :as shard]
            [elephantdb.common.config :as conf]
            [elephantdb.common.log :as log])
  (:import [elephantdb.generated ElephantDB$Iface WrongHostException
            DomainNotFoundException DomainNotLoadedException]
           [org.apache.thrift TException])
  (:gen-class :init init
              :implements [elephantdb.iface.IElephantClient]
              :constructors {[elephantdb.generated.ElephantDB$Iface java.util.Map
                              java.util.Map] []
                              [java.util.Map String] []}
              :state state))

(defn -init
  ([fs-conf global-conf-path]
     (-init nil fs-conf (conf/read-clj-config (filesystem fs-conf)
                                              global-conf-path)))
  ([local-elephant fs-conf global-conf]
     (let [{:keys [domains hosts replication]} global-conf]
       [[] {:local-hostname (local-hostname)
            :local-elephant local-elephant
            :global-conf global-conf       
            :domain-shard-indexes (shard/shard-domains (filesystem fs-conf)
                                                       domains
                                                       hosts
                                                       replication)}])))

(defn- get-index [this domain]
  (if-let [index (-> (.state this)
                     (:domain-shard-indexes)
                     (get domain))]
    index
    (throw (domain-not-found-ex domain))))

(defn- my-local-elephant [this]
  (-> this .state :local-elephant))

(defn- my-local-hostname [this]
  (-> this .state :local-hostname))

(defn- get-priority-hosts [this domain key]
  (let [hosts (shuffle (shard/key-hosts domain
                                        (get-index this domain)
                                        key))
        localhost (my-local-hostname this)]
    (if (some #{localhost} hosts)
      (cons localhost (remove-val localhost hosts))
      hosts)))

(defn- ring-port [this]
  (-> this .state :global-conf :port))

(defn multi-get-remote
  {:dynamic true}
  [host port domain keys]
  (with-elephant-connection host port client
    (.directMultiGet client domain keys)))

(defn- try-multi-get [this domain keys totry]
  (let [suffix (format "%s:%s/%s" totry domain keys)]
    (try (if (and (my-local-elephant this)
                  (= totry (my-local-hostname this)))
           (.directMultiGet (my-local-elephant this) domain keys)
           (multi-get-remote totry (ring-port this) domain keys))
         (catch TException e
           ;; try next host
           (log/error e "Thrift exception on " suffix))
         (catch WrongHostException e
           (log/error e "Fatal exception on " suffix)
           (throw (TException. "Fatal exception when performing get" e)))
         (catch DomainNotFoundException e
           (log/error e "Could not find domain when executing read on " suffix)
           (throw e))
         (catch DomainNotLoadedException e
           (log/error e "Domain not loaded when executing read on " suffix)
           (throw e)))))

(defn -get [this domain key]
  (first (.multiGet this domain [(serialize key)])))

(defn- host-indexed-keys
  "returns [hosts-to-try global-index key all-hosts] seq"
  [this domain keys]
  (for [[gi key] (map-indexed vector keys)
        :let [priority-hosts (get-priority-hosts this domain key)]]
    [priority-hosts gi key priority-hosts]))

(defn- multi-get*
  "executes multi-get, returns seq of [global-index val]"
  [this domain host host-indexed-keys key-shard-fn multi-get-remote-fn]
  (binding [shard/key-shard key-shard-fn
            multi-get-remote multi-get-remote-fn]
    (when-let [vals (try-multi-get this domain (map third host-indexed-keys) host)]
      (map (fn [v [hosts gi key all-hosts]] [gi v])
           vals
           host-indexed-keys))))

(defn -multiGet [this domain keys]
  (let [keys              (map serialize keys)
        host-indexed-keys (host-indexed-keys this domain keys)]
    (loop [keys-to-get host-indexed-keys
           results []]
      (let [host-map (group-by ffirst keys-to-get)
            rets (parallel-exec
                  (for [[host host-indexed-keys] host-map]
                    (constantly
                     [host (multi-get* this
                                       domain
                                       host
                                       host-indexed-keys
                                       shard/key-shard
                                       multi-get-remote)])))
            succeeded       (filter second rets)
            succeeded-hosts (map first succeeded)
            results (->> (map second succeeded)
                         (apply concat results))
            failed-host-map (apply dissoc host-map succeeded-hosts)]
        (if (empty? failed-host-map)
          (map second (sort-by first results))
          (recur (for [[[_ & hosts] gi key all-hosts]
                       (apply concat (vals failed-host-map))]
                   (if (empty? hosts)
                     (throw (hosts-down-ex all-hosts))
                     [hosts gi key all-hosts]))
                 results))))))
