(ns elephantdb.common.example
  "Namespace for example calls. Nothing actually gets bound here.")

(comment
  ;; this returns a 
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
    (-> db :shard-scheme (.shardIdx key)))

  (def full-db
    ;; We can get rid of some of these atoms if we maintain functions
    ;; for querying status, for the less common operations.
    {:coordinator <instance of Coordinator>
     :shard-scheme <shardscheme-instance>
     :shard-count <shard-count>
     :serializer <kryo-instance>
     :domain-config {:k1 "val1"} ;; user defined
     :domains {"some-domain" {:shard-index {:hosts-to-shards <map>
                                            :shards-to-hosts <map>}
                              :status <status-atom>
                              :local-handle "local-fs-handle"
                              :remote-handle "remote-fs-handle"
                              :domain-data <data-atom>
                              :current-version 12356456324
                              :all-versions #{{:id 12356456324, :status :open}
                                              {:id 123235324,   :status :downloading}
                                              {:id 1234534123,  :status :ready}}}
               ;; or this, for all-versions
               "other-domain" {:shard-index {:hosts-to-shards <map>
                                             :shards-to-hosts <map>}
                               :status <status-atom>
                               :local-handle "local-fs-handle"
                               :remote-handle "remote-fs-handle"
                               :domain-data <data-atom>
                               :current-version 12356456324
                               :all-versions {123235324 {:status :downloading}
                                              1234534123 {:status :ready}}}}})
  
  (defn locations
    "Returns a sequence of locations for the supplied key."
    [db domain key]
    (let [shard-idx (shard-idx db key)
          shard-map (-> db (get domain) :shard-index :shards-to-hosts)]
      (get shard-map shard))))


(comment
  "Wish list of things that could be pluggable:"
  ["filesystem representation"]
  ["Logger -- implement Logger protocol"])
