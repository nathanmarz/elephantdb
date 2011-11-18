(ns elephantdb.keyval.client
  )
;; (:refer-clojure :exclude (get))
;;   (:use elephantdb.keyval.config
;;         elephantdb.keyval.thrift
;;         elephantdb.common.hadoop
;;         elephantdb.common.util
;;         [elephantdb.common.types :only (serialize)]
;;         hadoop-util.core)
;;   (:require [elephantdb.common.shard :as shard]
;;             [elephantdb.common.config :as conf]
;;             [elephantdb.common.log :as log])
;;   (:import [elephantdb.generated ElephantDB$Iface WrongHostException
;;             DomainNotFoundException DomainNotLoadedException]
;;            [org.apache.thrift TException])
;;   (:gen-class :init init
;;               :implements [elephantdb.iface.IElephantClient]
;;               :constructors {[java.util.Map String] []
;;                              [java.util.Map
;;                               java.util.Map
;;                               elephantdb.generated.ElephantDB$Iface] []}
;;               :state state)

;; (def example-shard-index
;;   {"index-a" {::hosts-to-shards {"host1" #{1, 3}, "host2" #{2, 4}}
;;               ::shards-to-hosts {1 #{"host1"}, 2 #{"host2"},
;;                                  3 #{"host1"}, 4 #{"host2"}}}})

;; (defrecord ElephantClient
;;     [local-hostname local-elephant global-conf domain-shard-indexes])

;; (defn mk-client
;;   "fs-conf is a map meant to create a hadoop Filesystem object. For
;;    AWS, this usually includes `fs.default.name`,
;;    `fs.s3n.awsAccessKeyId` and `fs.s3n.awsSecretAccesskey`.

;;    global-conf-path: path to global-conf.clj on the filesystem
;;    specified by fs-conf.

;;    local-elephant: instance returned by elephantdb.keyval/edb-proxy."
;;   ([fs-conf global-conf-path]
;;      (mk-client fs-conf
;;                 (conf/read-clj-config (filesystem fs-conf)
;;                                       global-conf-path)
;;                 nil))
;;   ([fs-conf global-conf local-elephant]
;;      (let [{:keys [domains hosts replication]} global-conf]
;;        (ElephantClient. (local-hostname)
;;                         local-elephant
;;                         global-conf
;;                         (shard/shard-domains (filesystem fs-conf)
;;                                              domains
;;                                              hosts
;;                                              replication)))))

;; ;; This is really the only function that matters for ElephantDB
;; ;;   Key-Value; all others are just views on this. And THIS only depends
;; ;;   on directMultiGet.

;; (defn- host-indexed-keys
;;   "returns [hosts-to-try global-index key all-hosts] seq"
;;   [this domain keys]
;;   (for [[gi key] (map-indexed vector keys)
;;         :let [priority-hosts (get-priority-hosts this domain key)]]
;;     [priority-hosts gi key priority-hosts]))

;; (defn- multi-get*
;;   "executes multi-get, returns seq of [global-index val]"
;;   [this domain host host-indexed-keys key-shard-fn multi-get-remote-fn]
;;   (binding [shard/key-shard  key-shard-fn
;;             multi-get-remote multi-get-remote-fn]
;;     (when-let [vals (try-multi-get this domain (map third host-indexed-keys) host)]
;;       (map (fn [v [hosts gi key all-hosts]] [gi v])
;;            vals
;;            host-indexed-keys))))

;; (def thrift-functions
;;   {:get       (fn [this domain key]
;;                 (first (multiGet this domain [key])))
;;    :getInt    (fn [this domain key]
;;                 (get this domain (serialize key)))
;;    :getLong   (fn [this domain key]
;;                 (get this domain (serialize key)))
;;    :getString (fn [this domain key]
;;                 (get this domain (serialize key)))
;;    :multiGet  (fn [this domain keys]
;;                 (let [host-indexed-keys (host-indexed-keys this domain keys)]
;;                   (loop [keys-to-get host-indexed-keys
;;                          results []]
;;                     (let [host-map (group-by ffirst keys-to-get)
;;                           rets (parallel-exec
;;                                 (for [[host host-indexed-keys] host-map]
;;                                   (constantly
;;                                    [host (multi-get* this
;;                                                      domain
;;                                                      host
;;                                                      host-indexed-keys
;;                                                      shard/key-shard
;;                                                      multi-get-remote)])))
;;                           succeeded       (filter second rets)
;;                           succeeded-hosts (map first succeeded)
;;                           results (->> (map second succeeded)
;;                                        (apply concat results))
;;                           failed-host-map (apply dissoc host-map succeeded-hosts)]
;;                       (if (empty? failed-host-map)
;;                         (map second (sort-by first results))
;;                         (recur (for [[[_ & hosts] gi key all-hosts]
;;                                      (apply concat (vals failed-host-map))]
;;                                  (if (empty? hosts)
;;                                    (throw (hosts-down-ex all-hosts))
;;                                    [hosts gi key all-hosts]))
;;                                results))))))
;;    :multiGetInt    (fn [this domain keys]
;;                      (multiGet this domain (map serialize keys)))
;;    :multiGetLong   (fn [this domain keys]
;;                      (multiGet this domain (map serialize keys)))
;;    :multiGetString (fn [this domain keys]
;;                      (multiGet this domain (map serialize integers)))})
