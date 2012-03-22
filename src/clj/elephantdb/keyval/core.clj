(ns elephantdb.keyval.core
  "Functions for connecting the an ElephantDB (key-value) service via
  Thrift."
  (:use [elephantdb.common.domain :only (loaded?)])
  (:require [clojure.string :as s]
            [elephantdb.keyval.finagle :as f]
            [jackknife.core :as u]
            [jackknife.logging :as log]
            [jackknife.seq :as  seq]
            [elephantdb.common.database :as db]
            [elephantdb.common.shard :as shard]
            [elephantdb.common.status :as status]
            [elephantdb.common.thrift :as thrift]
            [elephantdb.common.config :as conf]
            [elephantdb.keyval.domain :as dom])
  (:import [com.twitter.util Future]
           [java.nio ByteBuffer]
           [org.apache.thrift.protocol TBinaryProtocol TBinaryProtocol$Factory]
           [org.apache.thrift.protocol ]
           [org.apache.thrift.transport TTransport]
           [org.apache.thrift TException]
           [elephantdb.common.database Database]
           [elephantdb.common.domain Domain]
           [elephantdb.generated DomainNotFoundException
            DomainNotLoadedException WrongHostException]
           [elephantdb.generated.keyval ElephantDB$Client 
            ElephantDB$ServiceIface ElephantDB$Processor ElephantDB$Service])
  (:gen-class))

;; ## Thrift Connection

(defn kv-client [transport]
  (ElephantDB$Client. (TBinaryProtocol. transport)))

(defn kv-processor
  "Returns a key-value thrift processor suitable for passing into
  launch-server!"
  [service-handler]
  (ElephantDB$Processor. service-handler))

(defmacro with-kv-connection
  [host port client-sym & body]
  `(with-open [^TTransport conn# (doto (thrift/thrift-transport ~host ~port)
                                   (.open))]
     (let [^ElephantDB$Client ~client-sym (kv-client conn#)]
       ~@body)))

;; ## Service Handler

(defn try-direct-multi-get
  "Attempts a direct multi-get to the supplied service for each of the
  keys in the supplied `key-seq`."
  [^ElephantDB$ServiceIface service domain-name error-suffix key-seq]
  (try (.directMultiGet service domain-name key-seq)
       (catch TException e
         (log/error e "Thrift exception on " error-suffix)) ;; try next host
       (catch WrongHostException e
         (log/error e "Fatal exception on " error-suffix)
         (throw (TException. "Fatal exception when performing get" e)))
       (catch DomainNotFoundException e
         (log/error e "Could not find domain when executing read on " error-suffix)
         (throw e))
       (catch DomainNotLoadedException e
         (log/error e "Domain not loaded when executing read on " error-suffix)
         (throw e))))

(defn try-kryo-multi-get
  "Attempts a direct multi-get to the supplied service for each of the
  keys in the supplied `key-seq`."
  [^ElephantDB$ServiceIface service database domain-name error-suffix key-seq]
  (try (let [^Domain dom (db/domain-get database domain-name)
             serializer  (.serializer dom)
             key-seq     (map (fn [x]
                                (ByteBuffer/wrap
                                 (.serialize serializer x)))
                              key-seq)] 
         (.directKryoMultiGet service domain-name key-seq))
       (catch TException e
         (log/error e "Thrift exception on " error-suffix)) ;; try next host
       (catch WrongHostException e
         (log/error e "Fatal exception on " error-suffix)
         (throw (TException. "Fatal exception when performing get" e)))
       (catch DomainNotFoundException e
         (log/error e "Could not find domain when executing read on " error-suffix)
         (throw e))
       (catch DomainNotLoadedException e
         (log/error e "Domain not loaded when executing read on " error-suffix)
         (throw e))))

;; multi-get* recieves a sequence of indexed-keys. Each of these is a
;; map with :index, :key and :host keys. On success, it returns the
;; indexed-keys input with :value keys associated onto every map. On
;; failure it throws an exception, or returns nil.

(defn multi-get*
  [service domain-name database localhost hostname indexed-keys]
  (let [port     (:port database)
        key-seq  (map :key indexed-keys)
        suffix   (format "%s:%s/%s" hostname domain-name key-seq)]
    (when-let [vals (if (= localhost hostname)
                      (try-direct-multi-get service
                                            domain-name
                                            suffix
                                            key-seq)
                      (with-kv-connection hostname port remote-service
                        (try-kryo-multi-get remote-service
                                            database
                                            domain-name
                                            suffix
                                            key-seq)))]
      (map (fn [m v] (assoc m :value v))
           indexed-keys
           vals))))

;; TODO: Fix this with a macro that lets us specify these behaviours
;; by default, but replace as necessary.

(defmacro defservice []
  "Something like this. Model the macro after defcache in
  clojure.core.cache.")

;; TODO: Perfect example of a spot where we could throw a data
;; structure warning up with throw+ if the database isn't loaded.

(defn direct-multiget [database domain-name key-seq]
  (let [domain (db/domain-get database domain-name)]
    (when (loaded? domain)
      (map (partial dom/kv-get domain)
           key-seq))))

;; ## MultiGet
;;
;; Start out by indexing each key; this requires indexing each key
;; into a map (see `index-keys` above). The loop first checks that
;; every key has at least one host associated with it. If any key is
;; lacking hosts, the multiGet throws an exception.
;;
;; If no exception is thrown, the system groups keys by the first host
;; in the list (localhost, if any keys are located on the machine
;; executing the call) and performs a directMultiGet on each for its
;; keys.
;;
;; If any host has unsuccessful results (didn't return anything), the
;; host is removed from the host lists of every key, and multiGet
;; recurses.
;;
;; Once the multi-get loop completes without any failures the entire
;; sequence of values is returned in order.

(defn multi-get
  [get-fn database domain-name key-seq]
  (loop [results []
         indexed-keys (-> (db/domain-get database domain-name)
                          (dom/index-keys key-seq))]
    (if-let [bad-key (some (comp empty? :hosts) indexed-keys)]
      (throw (thrift/hosts-down-ex (:all-hosts bad-key)))
      (let [host-map   (group-by (comp first :hosts) indexed-keys)
            rets       (u/do-pmap (fn [[host indexed-keys]]
                                    [host (get-fn host indexed-keys)])
                                  host-map)
            successful (into {} (filter second rets))
            results    (->> (vals successful)
                            (apply concat results))
            fail-map   (apply dissoc host-map (keys successful))]
        
        (if (empty? fail-map)
          (map :value (sort-by :index results))
          (recur results
                 (map (fn [m]
                        (update-in m [:hosts]
                                   dom/trim-hosts (keys fail-map)))
                      (apply concat (vals fail-map)))))))))

(defn kv-get-fn
  [service domain-name database]
  (partial multi-get*
           service
           domain-name
           database
           (u/local-hostname)))

;; TODO: Catch errors if we're not dealing specifically with a byte array.

(deftype KeyValDatabase [database]
  ElephantDB$ServiceIface
  (directKryoMultiGet [_ domain-name keys]
    (thrift/assert-domain database domain-name)
    (try (let [^Domain dom (db/domain-get database domain-name)
               serializer (.serializer dom)
               key-seq    (map (fn [^ByteBuffer x]
                                 (let [ret (byte-array (.remaining x))]
                                   (.get x ret)
                                   (.deserialize serializer ret)))
                               keys)]
           (if-let [val-seq (direct-multiget database domain-name key-seq)]
             (Future/value (doall (map thrift/mk-value val-seq)))
             (throw (thrift/domain-not-loaded-ex))))
         (catch RuntimeException e
           (log/error e "Thrown by directKryoMultiGet.")
           (throw (thrift/wrong-host-ex)))))

  (directMultiGet [_ domain-name keys]
    (thrift/assert-domain database domain-name)
    (try (if-let [val-seq (direct-multiget database domain-name keys)]
           (Future/value (doall (map thrift/mk-value val-seq)))
           (throw (thrift/domain-not-loaded-ex)))
         (catch RuntimeException _
           (throw (thrift/wrong-host-ex)))))

  (multiGet [this domain-name key-seq]
    (thrift/assert-domain database domain-name)
    (let [get-fn (kv-get-fn this domain-name database)]
      (multi-get get-fn
                 database
                 domain-name
                 (map (fn [^ByteBuffer x] (.array x))
                      key-seq))))

  (multiGetInt [this domain-name key-seq]
    (thrift/assert-domain database domain-name)
    (let [get-fn (kv-get-fn this domain-name database)]
      (multi-get get-fn database domain-name key-seq)))

  (multiGetLong [this domain-name key-seq]
    (thrift/assert-domain database domain-name)
    (let [get-fn (kv-get-fn this domain-name database)]
      (multi-get get-fn database domain-name key-seq)))
  
  (multiGetString [this domain-name key-seq]
    (thrift/assert-domain database domain-name)
    (let [get-fn (kv-get-fn this domain-name database)]
      (multi-get get-fn database domain-name key-seq)))

  (get [this domain-name key]
    (thrift/assert-domain database domain-name)
    (let [get-fn (kv-get-fn this domain-name database)]
      (first (multi-get get-fn database domain-name [(.array key)]))))
  
  (getInt [this domain-name key]
    (thrift/assert-domain database domain-name)
    (let [get-fn (kv-get-fn this domain-name database)]
      (first (multi-get get-fn database domain-name [key]))))

  (getLong [this domain-name key]
    (thrift/assert-domain database domain-name)
    (let [get-fn (kv-get-fn this domain-name database)]
      (first (multi-get get-fn database domain-name [key]))))

  (getString [this domain-name key]
    (thrift/assert-domain database domain-name)
    (let [get-fn (kv-get-fn this domain-name database)]
      (first (multi-get get-fn database domain-name [key]))))
  
  (getDomainStatus [_ domain-name]
    "Returns the thrift status of the supplied domain-name."
    (thrift/assert-domain database domain-name)
    (-> (db/domain-get database domain-name)
        (status/get-status)
        (thrift/to-thrift)))
  
  (getDomains [_]
    "Returns a sequence of all domain names being served."
    (db/domain-names database))

  (getStatus [_]
    "Returns a map of domain-name->status for each domain."
    (thrift/elephant-status
     (u/update-vals (db/domain->status database)
                    (fn [_ status] (thrift/to-thrift status)))))

  (isFullyLoaded [_]
    "Are all domains loaded properly?"
    (db/fully-loaded? database))

  (isUpdating [_]
    "Is some domain currently updating?"
    (db/some-loading? database))

  (update [_ domain-name]
    "If an update is available, updates the named domain and
         hotswaps the new version."
    (thrift/assert-domain database domain-name)
    (u/with-ret true
      (db/attempt-update! database domain-name)))

  (updateAll [_]
    "If an update is available on any domain, updates the domain's
         shards from its remote store and hotswaps in the new versions."
    (u/with-ret true
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
        service  (ElephantDB$Service. (KeyValDatabase. database)
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
  (let [database (doto (db/build-database
                        {:local-root "/tmp/dbroot"
                         :domains {"clicks" "/tmp/domain1"}})
                   (db/update-all!))
        server-a  (-> (KeyValDatabase. database)
                      (ElephantDB$Service. (TBinaryProtocol$Factory.))
                      (thrift/launch-server! 3500))
        server-b  (-> (KeyValDatabase. database)
                      (ElephantDB$Service. (TBinaryProtocol$Factory.))
                      (thrift/launch-server! 3600))]
    (try  (prn server-a ", " server-b)
          (finally (elephantdb.keyval.finagle/kill! server-a)
                   (elephantdb.keyval.finagle/kill! server-b))))

  (def db
    "Returns an example database on my filesystem."
    (doto (db/build-database
           {:local-root "/tmp/dbroot"
            :domains {"clicks" "/tmp/domain1"}})
      (db/update-all!)))

  (def hosts->clients
    "Returns a map from a particular set of hosts to a client tuned to
    access those hosts."
    (let [localhost (u/local-hostname)]
      (->> (for [domain-name (db/domain-names db)
                 :let [dom   (db/domain-get db domain-name)]
                 [_ host-seq]   (shard/shards->hosts (.shardIndex dom))]
             host-seq)
           (distinct)
           (map (fn [host-set]
                  (let [host-seq (->> (shuffle host-set)
                                      (seq/prioritize #{localhost}))
                        host-str (s/join "," (map #(str % ":" (:port db)) host-seq))]
                    [host-set (f/create-client host-str)])))
           (into {}))))

  (defn shard->client [client-map domain shard-idx]
    (get client-map
         (shard/host-set (.shardIndex domain)
                         shard-idx)))

  (defn db-shard->client
    [client-map db domain-name shard-idx]
    (let [domain (db/domain-get db domain-name)]
      (shard->client client-map domain shard-idx)))
  )
