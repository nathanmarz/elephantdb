(ns elephantdb.common.shard
  (:use [elephantdb.common.config :only (read-domain-spec)])
  (:require [elephantdb.common.util :as u]
            [clojure.string :as s]
            [elephantdb.common.log :as log])
  (:import [elephantdb Utils]))

(defn- host-shard-assigner
  [[hosts hosts-to-shards] shard]
  (let [[host & hosts] (drop-while #(get-in hosts-to-shards
                                            [% shard])
                                   hosts)
        existing (get hosts-to-shards host #{})]
    [hosts (->> (conj existing shard)
                (assoc hosts-to-shards host))]))

(defn compute-host-to-shards
  "Returns a map of host-> shard set. For example:

  (compute-host-to-shards 5 [\"a\" \"b\"] 1)
  ;=> {\"b\" #{1 3}, \"a\" #{0 2 4}}"
  {:dynamic true}
  [shard-count hosts replication]
  (log/info "host->shards: " (s/join "," [shard-count hosts replication]))
  (u/safe-assert (>= (count hosts) replication)
                 "Replication greater than number of servers")
  (->> (u/repeat-seq replication (range shard-count))
       (reduce host-shard-assigner [(cycle hosts) {}])
       (second)))

(defn- shard-domain
  "Shard a single domain."
  [shard-count hosts replication]
  (let [hosts-to-shards (compute-host-to-shards shard-count
                                                hosts
                                                replication)]
    {::hosts-to-shards hosts-to-shards
     ::shards-to-hosts (->> (u/reverse-multimap hosts-to-shards)
                            (u/val-map set))}))

(defn shard-domains
  "TODO: Test that we don't get a FAILURE if the domain-spec doesn't
  exist."
  [fs domain-map hosts replication]
  (log/info "Sharding domains:" (keys domain-map))
  (u/update-vals (fn [domain remote-location]
                   (let [{:keys [num-shards]}
                         (read-domain-spec fs remote-location)]
                     (log/info "Sharding domain " domain)
                     (shard-domain num-shards hosts replication)))
                 domain-map))

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
  "For the supplied domain, shard index and key, returns a set of all
  hosts containing the supplied key.

  TODO: Call serialize here, to push it down as far as possible?"
  [domain index ^bytes key]
  (->> (num-shards index)
       (key-shard domain key)
       (shard-hosts index)))
