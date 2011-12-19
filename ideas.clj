"interface ideas!"
;; Domains could also implement comparable... Anyway, the idea here is
;; that we need something that implements IDomainStore to contain
;; shards.

;; domain store, either remote or local, that should
;; be able to provide paths and connections within its guts (but have
;; no real knowledge of the filesyste, etc.


;; The IDomainStore interface is service-facing; actual elephantDB
;; applications will need a lower-level interface that reaches into
;; these domain stores to provide access to individual shard paths.
;;
;; On the other hand, we might find that it's enough to provide a path
;; to the latest version; if a "version" can be fully synched between
;; between filesystems then we're good to go.
;;
;; The filesystem implementation should not be tied to hadoop. With an
;; interface like this it becomes possible to create varying records
;; and types; if they fulfill this interface, and the interface
;; required for transfer, that should be enough.
;;
;; Protocol examples: https://gist.github.com/1495818

(defprotocol IDomainStore
  (allVersions [_]
    "Returns a sequence of available version timestamps.")
  (latestVersion [_]
    "Returns the timestamp (in seconds) of the latest versions.")
  (latestVersionPath [_]
    "Returns the timestamp (in seconds) of the latest versions.")
  (hasData [_]
    "Does the domain contain any domains at all?")
  (cleanup [_ to-keep]
    "Destroys all but the last `to-keep` versions.")
  (spec [_]
    "Returns a clojure datastructure containing the DomainStore's spec."))

(def example-db
  (get-database {:host "localhost"
                 :port "5378" ;; by default
                 :username "" ;; can we authenticate?
                 :password ""
                 }))

;; Example function using the client interface
(defn user-info [username]
  (with-db example-db
    (let [vals (multi-get "name_sritchie" "age_sritchie" "gender_sritchie")]
      (zipmap [:name :age :gender] vals))))

;; For key-value, we won't require getString, getInt etc:
(with-db example-db
  (println (get 100))
  (println (get "string!"))
  (println (get 1123656123356343)))

;; database status:
(domain-status example-db)
;; :shutdown, :loading, :ready, :failed ;; with info, perhaps?

(domains example-db)
;; not sure which is best:
{"some-domain" {:shard-index {:hosts-to-shards <map>
                              :shards-to-hosts <map>}
                :status <status-atom>
                :domain-data <data-atom>
                :current-version 12356456324
                :all-versions #{{:id 12356456324, :status :open}
                                {:id 123235324,    :status :downloading}
                                {:id 1234534123,   :status :ready}}}
 ;; or this, for all-versions
 "other-domain" {:shard-index {:hosts-to-shards <map>
                               :shards-to-hosts <map>}
                 :status <status-atom>
                 :domain-data <data-atom>
                 :current-version 12356456324
                 :all-versions {123235324 {:status :downloading}
                                1234534123 {:status :ready}}}}

(load-version! example-db "domain" 123235324)
;; returns domain status on completion; triggers swaps on remote
;; machines as well.

;; that all needs to be sugar over a lower layer, I think. A
;; database consists of:

;; 1. A series of "Domains", each of which has a bucket of shards on
;; any number of machines.
;;
;; 2. A "Domain Spec":
;;    1. :persistence-coordinator
;;    2. :shard-count
;;    3. :serializer
;;    4. :shard-scheme
;;    5. :domain-config --> You need to be able to store
;; configuration parameters in the domain spec and pass them back
;; and forth. Say your user decided to shard on a specific field
;; inside of the lucene document; on the hadoop side, your code
;; would have pulled the field name out. You need to be able to
;; access your argument configuration on the client side.

;; :shard-scheme should expose
(defn shard-idx [db key]
  (-> db :shard-scheme (.shardIndex key)))

(def full-db
  ;; We can get rid of some of these atoms if we maintain functions
  ;; for querying status, for the less common operations.
  {:port 3578
   :local-root <local-path>
   :download-cap 1024
   :hdfs-conf {"fs.default.name" "hdfs://hadoop-devel-nn.local.twitter.com:8020"}
   :blob-conf {"fs.default.name" "hdfs://hadoop-devel-nn.local.twitter.com:8020"}
   :domains {"some-domain" {:coordinator <instance of Coordinator>
                            :shard-scheme <shardscheme-instance>
                            :status <status-atom>
                            :shard-count <shard-count>
                            :serializer <kryo-instance>
                            :local-handle "local-fs-handle"
                            :remote-handle "remote-fs-handle"
                            :domain-data <data-atom>
                            :shard-index {:hosts-to-shards <map>
                                          :shards-to-hosts <map>}#
                            :current-version 12356456324
                            :version-map #{{:id 12356456324, :status :open}
                                           {:id 123235324,   :status :downloading}
                                           {:id 1234534123,  :status :ready}}}}})

(defn locations
  "Returns a sequence of locations for the supplied key."
  [db domain key]
  (let [shard-idx (shard-idx db key)
        shard-map (-> db (get domain) :shard-index :shards-to-hosts)]
    (get shard-map shard)))


(comment
  "Wish list of things that could be pluggable:"
  ["Logger -- implement Logger protocol?"])
