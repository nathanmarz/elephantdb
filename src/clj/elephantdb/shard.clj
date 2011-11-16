(ns elephantdb.shard
  (:use elephantdb.config
        elephantdb.common.util)
  (:require [clojure.string :as s]
            [elephantdb.common.log :as log])
  (:import [elephantdb Utils]))

;; ## This namespace 

(defstruct shard-index ::hosts-to-shards ::shards-to-hosts)

(defn- host-shard-assigner
  [[hosts hosts-to-shards] shard]
  (let [[host & hosts] (drop-while #(get-in hosts-to-shards
                                            [% shard])
                                   hosts)
        existing (get hosts-to-shards host #{})]
    [hosts (->> (conj existing shard)
                (assoc hosts-to-shards host))]))

(defn shard-log [s domain hosts numshards replication]
  (log/info (s/join ", " [s domain hosts numshards replication])))

(defn compute-host-to-shards
  {:dynamic true}
  [domain hosts numshards replication]
  (shard-log "host to shards" domain hosts numshards replication)
  (safe-assert (>= (count hosts) replication)
               "Replication greater than number of servers")
  (->> (repeat-seq replication (range numshards))
       (reduce host-shard-assigner [(cycle hosts) {}])
       (second)))

(defn- shard-domain
  "Shard a single domain."
  [hosts replication shard-count domain]
  (shard-log "sharding domain" domain hosts shard-count replication)
  (let [hosts-to-shards (compute-host-to-shards domain hosts shard-count replication)]
    (-> (reverse-multimap hosts-to-shards)
        (map-mapvals set)
        (->> (struct shard-index hosts-to-shards)))))

(defn shard-domains
  "TODO: Test that we don't get a FAILURE if the domain-spec doesn't
  exist."
  [fs domains hosts replication]
  (let [sharder (partial shard-domain hosts replication)]
    (log/info "Sharding domains...")
    (update-vals domains
                 (fn [domain remote-location]
                   (let [{shards :num-shards :as domain-spec}
                         (read-domain-spec fs remote-location)]
                     (sharder shards domain))))))

(defn host-shards [index host]
  (get (::hosts-to-shards index) host))

(defn shard-hosts [index shard]
  (get (::shards-to-hosts index) shard))

(defn num-shards [index]
  (count (keys (::shards-to-hosts index))))

(defn key-shard
  {:dynamic true}
  [domain key amt]
  (Utils/keyShard key amt))

(defn key-hosts
  [domain index #^bytes key]
  (->> (num-shards index)
       (key-shard domain key)
       (shard-hosts index)))
