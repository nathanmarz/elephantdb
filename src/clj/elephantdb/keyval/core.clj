(ns elephantdb.keyval.core
  "Functions for connecting the an ElephantDB (key-value) service via
  Thrift."
  (:use [elephantdb.common.domain :only (loaded? key->shard)]
        [elephantdb.common.status :only (get-status)])
  (:require elephantdb.keyval.finagle
            [clojure.string :as s]
            [jackknife.core :as u]
            [jackknife.logging :as log]
            [jackknife.seq :as  seq]
            [elephantdb.common.database :as db]
            [elephantdb.common.shard :as shard]
            [elephantdb.common.thrift :as thrift]
            [elephantdb.common.config :as conf]
            [elephantdb.keyval.domain :as dom])
  (:import [java.nio ByteBuffer]
           [com.twitter.util Future]
           [com.twitter.finagle.builder ClientBuilder]
           [com.twitter.finagle.thrift ThriftClientFramedCodec]
           [org.apache.thrift.protocol TBinaryProtocol$Factory]
           [elephantdb.common.database Database]
           [elephantdb.common.domain Domain]
           [elephantdb.generated.keyval ElephantDB$ServiceToClient
            ElephantDB$ServiceIface ElephantDB$Service])
  (:gen-class))

(defn domain-get [db domain-name]
  (thrift/assert-domain db domain-name)
  (db/domain-get db domain-name))

;; ## Thrift Connection

(defn create-client [host-str & {:keys [conn-limit]}]
  (let [transport (-> (ClientBuilder/get)
                      (.hosts host-str)
                      (.codec (ThriftClientFramedCodec/get))
                      (.hostConnectionLimit (or conn-limit 100)))]
    (ElephantDB$ServiceToClient. (ClientBuilder/safeBuild transport)
                                 (TBinaryProtocol$Factory.))))

(defn hosts->clients
  "Returns a map from a particular set of hosts to a client tuned to
    access those hosts."
  [database & {:keys [port]
               :or {port (:port database)}}]
  (let [localhost (u/local-hostname)]
    (->> (for [domain-name (db/domain-names database)
               :let [dom   (db/domain-get database domain-name)]
               [_ host-seq]   (shard/shards->hosts (.shardIndex dom))]
           host-seq)
         (distinct)
         (map (fn [host-set]
                (let [host-seq (->> (shuffle host-set)
                                    (seq/prioritize #{localhost}))
                      host-str (s/join "," (map #(str % ":" port) host-seq))]
                  [host-set (create-client host-str)])))
         (into {}))))

;; ## Service Handler

;; TODO: Catch errors if we're not dealing specifically with a byte array.

(defn deserialize-keys [^Domain dom key-seq]
  (let [serializer  (.serializer dom)]
    (map (fn [^ByteBuffer x]
           (let [ret (byte-array (.remaining x))]
             (.get x ret)
             (.deserialize serializer ret)))
         key-seq)))

;; TODO: Work out proper errors. Are we sure that this is going to
;; throw a RuntimeException if the shard isn't located on this host?

(defn direct-multiget [domain key-seq]
  (if (loaded? domain)
    (try (Future/value
          (doall (map (comp thrift/mk-value
                            (partial dom/kv-get domain))
                      @key-seq)))
         (catch RuntimeException e
           (log/error e "Thrown by direct-multiget.")
           (throw (Future/exception (thrift/wrong-host-ex)))))
    (throw (Future/exception (thrift/domain-not-loaded-ex)))))

;; ## MAKING A MULTIGET request.

(defn shard->client
  "Accepts a service wrapper and returns the appropriate client to
    access the shard on the domain with the supplied name."
  [client-map domain shard-idx]
  (get client-map
       (shard/host-set (.shardIndex domain)
                       shard-idx)))
  
(defn index-keys [domain key-seq]
  (for [[idx key] (map-indexed vector key-seq)]
    {:index idx, :key key}))

(defn kryo-multi-get*
  "Attempts a direct multi-get to the supplied service for each of
     the keys in the supplied `key-seq`."
  [client serializer domain-name key-seq]
  (let [key-seq (map #(ByteBuffer/wrap
                       (.serialize serializer %))
                     key-seq)]
    (.directKryoMultiGet client domain-name key-seq)))

;; kryo-multi-get receives a sequence of indexed-keys. Each of these is a
;; map with :index and :key keys. On success, it returns the
;; indexed-keys input with :value pairs associated onto each map. On
;; failure it throws an exception, or returns nil.

(defn kryo-multi-get
  [client ^Domain domain domain-name indexed-keys]
  (let [serializer (.serializer domain)
        key-seq    (map :key indexed-keys)]
    (when-let [vals (.apply (kryo-multi-get* client
                                             serializer
                                             domain-name
                                             key-seq))]
      (map (fn [m v] (assoc m :value v))
           indexed-keys
           vals))))
  
(defn multi-get [client-map db domain-name key-seq]
  (let [dom (domain-get db domain-name)]
    (->>  (index-keys dom key-seq)
          (group-by #(key->shard dom (:key %)))
          (u/do-pmap
           (fn [[shard indexed-keys]]
             (let [client (shard->client client-map dom shard)]
               (kryo-multi-get client dom domain-name indexed-keys))))
          (apply concat)
          (sort-by :index)
          (map :value))))
  
(deftype KeyValDatabase [database client-map]
  ElephantDB$ServiceIface
  (directKryoMultiGet [_ domain-name keys]
    (thrift/assert-domain database domain-name)
    (let [domain (db/domain-get database domain-name)]
      (direct-multiget domain (delay (deserialize-keys domain keys)))))
  
  (directMultiGet [_ domain-name keys]
    (thrift/assert-domain database domain-name)
    (direct-multiget (db/domain-get database domain-name)
                     (delay keys)))

  (multiGet [this domain-name key-seq]
    (Future/value
     (multi-get client-map
                database
                domain-name
                (map (fn [^ByteBuffer x] (.array x))
                     key-seq))))

  (multiGetInt [_ domain-name key-seq]
    (Future/value
     (multi-get client-map database domain-name key-seq)))

  (multiGetLong [_ domain-name key-seq]
    (Future/value
     (multi-get client-map database domain-name key-seq)))
  
  (multiGetString [_ domain-name key-seq]
    (Future/value
     (multi-get client-map database domain-name key-seq)))

  (get [this domain-name key]
    (Future/value
     (first (multi-get client-map database domain-name [(.array key)]))))
  
  (getInt [this domain-name key]
    (Future/value
     (first (multi-get client-map database domain-name [key]))))

  (getLong [this domain-name key]
    (Future/value
     (first (multi-get client-map database domain-name [key]))))

  (getString [this domain-name key]
    (Future/value
     (first (multi-get client-map database domain-name [key]))))
  
  (getDomainStatus [_ domain-name]
    "Returns the thrift status of the supplied domain-name."
    (-> (domain-get database domain-name)
        (get-status)
        (thrift/to-thrift)
        (Future/value)))
  
  (getDomains [_]
    "Returns a sequence of all domain names being served."
    (Future/value (db/domain-names database)))

  (getStatus [_]
    "Returns a map of domain-name->status for each domain."
    (Future/value
     (thrift/elephant-status
      (u/update-vals (db/domain->status database)
                     (fn [_ status] (thrift/to-thrift status))))))

  (isFullyLoaded [_]
    "Are all domains loaded properly?"
    (Future/value (db/fully-loaded? database)))

  (isUpdating [_]
    "Is some domain currently updating?"
    (Future/value (db/some-loading? database)))

  (update [_ domain-name]
    "If an update is available, updates the named domain and
         hotswaps the new version."
    (thrift/assert-domain database domain-name)
    (u/with-ret (Future/value true)
      (db/attempt-update! database domain-name)))

  (updateAll [_]
    "If an update is available on any domain, updates the domain's
         shards from its remote store and hotswaps in the new versions."
    (u/with-ret (Future/value true)
      (db/update-all! database))))

;; # Main Access
;;
;; This namespace is the main access point to the edb
;; code. elephantdb.keyval/-main Boots up the ElephantDB service and
;; an updater process that watches all domains and trigger an atomic
;; update in the background when some new version appears.
;;
;; TODO: Booting needs a little work; I'll do this along with the
;; deploy.

(defn -main
  "Main booting function for all of EDB. Pass in:

  `global-config-hdfs-path`: the hdfs path of `global-config.clj`

  `local-config-path`: the path to `local-config.clj` on this machine."
  [global-config-hdfs-path local-config-path]
  (log/configure-logging "log4j/log4j.properties")
  (let [local-config   (conf/read-local-config  local-config-path)
        global-config  (conf/read-global-config global-config-hdfs-path
                                                local-config)
        conf-map (merge global-config local-config)
        database (db/build-database conf-map)
        clients  (hosts->clients database)
        service  (ElephantDB$Service. (KeyValDatabase. database clients)
                                      (TBinaryProtocol$Factory.))]
    (doto database
      (db/prepare)
      (db/launch-updater! (:update-interval-s conf-map)))
    (future (db/update-all! database))
    (thrift/launch-server! service (:port conf-map))))


;; ## Experimental Code

(comment
  (use 'elephantdb.test.keyval)
  (use 'elephantdb.test.common)

  (with-domain [my-domain (berkeley-spec 2)
                {1 2, 3 4}
                :version 5]
    (for [idx (-> my-domain .localStore .getSpec .getNumShards range)]
      idx))

  (mk-kv-domain (berkeley-spec 4) "/tmp/domain1"
                {1 (byte-array 10)
                 3 (byte-array 10)
                 5 (byte-array 10)
                 7 (byte-array 10)
                 9 (byte-array 10)})
  (def db
    (db/build-database
     {:local-root "/tmp/dbroot"
      :domains {"clicks" "/tmp/domain1"}}))

  ;; This bitch shows how to create multiple services on a single
  ;; machine.
  (let [database     (doto (db/build-database
                            {:local-root "/tmp/dbroot"
                             :domains {"clicks" "/tmp/domain1"}})
                       (db/update-all!))
        client-a-map (hosts->clients database :port 3500)
        client-b-map (hosts->clients database :port 3600)
        server-a     (-> (KeyValDatabase. database client-a-map)
                         (ElephantDB$Service. (TBinaryProtocol$Factory.))
                         (thrift/launch-server! 3500))
        server-b     (-> (KeyValDatabase. database client-b-map)
                         (ElephantDB$Service. (TBinaryProtocol$Factory.))
                         (thrift/launch-server! 3600))
        my-client    (create-client "localhost:3500,localhost:3600")]
    (try (.isDefined (.getLong my-client "clicks" 1))
         (finally (elephantdb.keyval.finagle/kill! server-a)
                  (elephantdb.keyval.finagle/kill! server-b))))

  (def db
    "Returns an example database on my filesystem."
    (doto (db/build-database
           {:local-root "/tmp/dbroot"
            :domains {"clicks" "/tmp/domain1"}})
      (db/update-all!)))

  (defn service-wrapper [database]
    {:database database
     :service  (-> (KeyValDatabase. database)
                   (ElephantDB$Service. (TBinaryProtocol$Factory.))
                   (thrift/launch-server! (:port database)))
     :clients  (hosts->clients database)})
  
  (defn FAKE-shard->client
    "Accepts a service wrapper and returns the appropriate client to
    access the shard on the domain with the supplied name."
    [{:keys [database clients]} domain-name shard-idx]
    (let [domain (db/domain-get database domain-name)]
      (get clients
           (shard/host-set (.shardIndex domain)
                           shard-idx)))))

(defn run-example []
  (let [database     (doto (db/build-database
                            {:local-root "/tmp/dbroot"
                             :domains {"clicks" "/tmp/domain1"}})
                       (db/update-all!))
        client-a-map (hosts->clients database :port 3500)
        client-b-map (hosts->clients database :port 3600)
        server-a     (-> (KeyValDatabase. database client-a-map)
                         (ElephantDB$Service. (TBinaryProtocol$Factory.))
                         (thrift/launch-server! 3500))
        server-b     (-> (KeyValDatabase. database client-b-map)
                         (ElephantDB$Service. (TBinaryProtocol$Factory.))
                         (thrift/launch-server! 3600))
        my-client    (create-client "localhost:3500,localhost:3600")]
    (try (.apply (.getLong my-client "clicks" 1))
         (finally (elephantdb.keyval.finagle/kill! server-a)
                  (elephantdb.keyval.finagle/kill! server-b)))))
