(ns elephantdb.shard
  (:use [elephantdb util])
  (:import [elephantdb Utils]))

(defstruct shard-index ::hosts-to-shards ::shards-to-hosts)

(defn- host-shard-assigner [[hosts hosts-to-shards] shard]
  (let [[host hosts] (find-first-next #(not ((get hosts-to-shards % #{}) shard)) hosts)
        existing (get hosts-to-shards host #{})]
    [hosts (assoc hosts-to-shards host (conj existing shard))]
    ))

(defn shard-domain [hosts numshards replication]
  (when (> replication (count hosts))
    (throw (IllegalArgumentException. "Replication greater than number of servers")))
  (let [shards (repeat-seq replication (range numshards))
        hosts-to-shards (second (reduce host-shard-assigner [(repeat-seq hosts) {}] shards))]
      (struct shard-index hosts-to-shards (map-mapvals set (reverse-multimap hosts-to-shards)))
    ))

(defn host-shards [index host]
  ((::hosts-to-shards index) host))

(defn shard-hosts [index shard]
  ((:shards-to-hosts index) shard))

(defn num-shards [index]
  (count (keys (::shards-to-hosts index))))

(defn key-hosts [index #^bytes key]
  (shard-hosts index (Utils/keyShard key (num-shards index))))
