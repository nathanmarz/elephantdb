(ns elephantdb.shard
  (:use [elephantdb util config log])
  (:import [elephantdb Utils]))

(defstruct shard-index ::hosts-to-shards ::shards-to-hosts)

(defn- host-shard-assigner [[hosts hosts-to-shards] shard]
  (let [[host hosts] (find-first-next #(not ((get hosts-to-shards % #{}) shard)) hosts)
        existing (get hosts-to-shards host #{})]
    [hosts (assoc hosts-to-shards host (conj existing shard))]))

(defn compute-host-to-shards [domain hosts numshards replication]
  (log-message "host to shards " domain hosts numshards replication)
  (when (> replication (count hosts))
    (throw (IllegalArgumentException. "Replication greater than number of servers")))
  (let [shards (repeat-seq replication (range numshards))
        hosts-to-shards (second (reduce host-shard-assigner [(repeat-seq hosts) {}] shards))]
    hosts-to-shards))

(defn shard-domain [domain hosts numshards replication]
  (log-message "sharding domain " domain hosts numshards replication)
  (let [hosts-to-shards (compute-host-to-shards domain hosts numshards replication)]
    (struct shard-index hosts-to-shards (map-mapvals set (reverse-multimap hosts-to-shards)))))

(defn shard-domains [fs global-config]
  (log-message "shard-domains" global-config)
  (->> (dofor [[domain remote-location] (:domains global-config)]
              (let [domain-spec (read-domain-spec fs remote-location)]
                (log-message "Domain spec for " domain ": " domain-spec)
                [domain (shard-domain
                         domain
                         (:hosts global-config)
                         (:num-shards domain-spec)
                         (:replication global-config))]))
       (into {})))

(defn host-shards [index host]
  ((::hosts-to-shards index) host))

(defn shard-hosts [index shard]
  ((::shards-to-hosts index) shard))

(defn num-shards [index]
  (count (keys (::shards-to-hosts index))))

(defn key-shard [domain key amt]
  (Utils/keyShard key amt))

(defn key-hosts
  [domain index #^bytes key]
  (->> (num-shards index)
       (key-shard domain key)
       (shard-hosts index)))
