(ns elephantdb.keyval.core
  "Functions for connecting the an ElephantDB (key-value) service via
  Thrift."
  (:require [elephantdb.common.database :as db]
            [elephantdb.common.thrift :as thrift])
  (:import [org.apache.thrift.protocol TBinaryProtocol]
           [org.apache.thrift.transport TTransport]
           [elephantdb.generated ElephantDB$Client Value]))

(defn mk-value
  "Wraps the supplied byte array in an instance of
  `elephantdb.generated.Value`."
  [val]
  (doto (Value.)
    (.set_data val)))

;; ## Thrift Connection

(defn kv-client [transport]
  (ElephantDB$Client. (TBinaryProtocol. transport)))

(defmacro with-kv-connection
  [host port client-sym & body]
  `(with-open [^TTransport conn# (thrift/thrift-transport ~host ~port)]
     (let [^ElephantDB$Client ~client-sym (kv-client conn#)]
       ~@body)))

;; ## Service Handler

(defn try-multi-get
  "Attempts a direct multi-get to the supplied service for each of the
  keys in the supplied `key-seq`."
  [^ElephantDB$Client db domain-name error-suffix key-seq]
  (try (.directMultiGet db domain-name key-seq)
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
  [database domain-name localhost hostname indexed-keys]
  (let [port     (:port database)
        key-seq  (map :keys indexed-keys)
        suffix   (format "%s:%s/%s" hostname domain-name key-seq)
        multiget #(try-multi-get % domain-name suffix key-seq)]
    (when-let [vals (if (= localhost hostname)
                      (multiget database)
                      (thrift/with-kv-connection hostname port remote-service
                        (multiget remote-service)))]
      (map (fn [m v] (assoc m :value v))
           indexed-keys
           vals))))

(defn multiget-fn
  [^ElephantDB$Iface database domain key-seq]
  (.multiGet database domain key-seq))

(defn get-fn [^ElephantDB$Iface database domain key]
  (first (.multiGet database domain [key])))

(def kv-aliases
  (merge (zipmap [:get :getInt :getLong :getString]
                 (repeat get-fn))
         (zipmap [:multiGetInt :multiGetLong :multiGetString]
                 (repeat get-fn))))


(extend db/Database
  ElephantDB$Iface
  (merge kv-aliases
         {:directMultiGet
          (fn [this domain-name keys]
            (assert-domain this domain-name)
            (try (let [domain (db/domain-get this domain-name)]
                   (doall
                    (map #(thrift/mk-value (kv-get domain %))
                         keys))
                   (catch RuntimeException _
                     (throw (thrift/wrong-host-ex))))))

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

          :multiGet
          (fn [this domain-name key-seq]
            (assert-domain this domain-name)
            (let [localhost (u/local-hostname)]
              (loop [indexed-keys (-> (db/domain-get this domain-name)
                                      (index-keys key-seq))
                     results []]
                (if-let [bad-key (some (comp empty? :hosts) indexed-keys)]
                  (throw (thrift/hosts-down-ex (:all-hosts bad-key)))
                  (let [host-map   (group-by (comp first :hosts) indexed-keys)
                        get-fn     (partial multi-get*
                                            this
                                            domain-name
                                            localhost)
                        rets       (u/do-pmap (fn [[host indexed-keys]]
                                                [host (get-fn host indexed-keys)])
                                              host-map)
                        successful (into {} (filter second rets))
                        results    (->> (vals successful)
                                        (apply concat results))
                        fail-map   (apply dissoc host-map (keys successful))]
                    (if (empty? fail-map)
                      (map :value (sort-by :index results))
                      (recur (map (fn [m]
                                    (update-in m [:hosts] trim-hosts (keys fail-map)))
                                  (apply concat (vals fail-map)))
                             results)))))))}))

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
  (let [local-config   (read-local-config  local-config-path)
        global-config  (read-global-config global-config-hdfs-path
                                           local-config)]
    (thrift/launch-database!
     (db/build-database (merge global-config local-config)))))
