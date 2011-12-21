(ns elephantdb.keyval.service
  (:use [elephantdb.keyval.thrift :only (with-elephant-connection)])
  (:require [jackknife.core :as u]
            [jackknife.logging :as log]
            [elephantdb.common.domain :as dom]
            [elephantdb.common.database :as db]
            [elephantdb.keyval.thrift :as thrift])
  (:import [org.apache.thrift TException]
           [org.apache.thrift.server THsHaServer THsHaServer$Options]
           [org.apache.thrift.protocol TBinaryProtocol$Factory]
           [org.apache.thrift.transport TNonblockingServerSocket]
           [java.util.concurrent.locks ReentrantReadWriteLock]
           [elephantdb.persistence Shutdownable]
           [elephantdb.generated ElephantDB ElephantDB$Iface
            ElephantDB$Processor WrongHostException
            DomainNotFoundException DomainNotLoadedException]))

;; ## Service Handler

(defn trim-hosts
    "Used within a multi-get's loop. Accepts a sequence of hosts + a
    sequence of hosts known to be bad, filters the bad hosts and drops
    the first one."
    [host-seq bad-hosts]
    (remove (set bad-hosts)
            (rest host-seq)))

(defn domain-get
  "Retrieves the requested domain (by name) from the supplied
  database. Throws a thrift exception if the domain doesn't exist."
  [database domain-name]
  (or (db/domain-get database domain-name)
      (thrift/domain-not-found-ex domain-name)))

(defn index-keys
  "For the supplied domain and sequence of keys, returns a sequence of
  maps with the following keys:

  :key   - the key.
  :index - the index in the original key sequence.
  :hosts - A sequence of hosts at which the key can be found.
  :all-hosts - the same list as hosts, at first. As gets are attempted
  on each key, the recursion will drop names from `hosts` and keep
  them around in `:all-hosts` for error reporting."
  [domain key-seq]
  (for [[idx key] (map-indexed vector key-seq)
        :let [hosts (dom/prioritize-hosts domain key)]]
    {:index idx, :key key, :hosts hosts, :all-hosts hosts}))

(defn try-multi-get
  "Attempts a direct multi-get to the supplied service for each of the
  keys in the supplied `key-seq`."
  [service domain-name error-suffix key-seq]
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

;; Into this function comes a sequence of indexed-keys. Each
;; of these is a map with :index, :key and :host keys. On
;; success, it returns the indexed-keys input with :value keys
;; associated onto every map. On failure it throws an
;; exception, or returns nil.

(defn multi-get*
  [service domain-name local-hostname hostname port indexed-keys]
  (let [key-seq  (map :keys indexed-keys)
        suffix   (format "%s:%s/%s" hostname domain-name key-seq)
        multiget #(try-multi-get % domain-name suffix key-seq)]
    (when-let [vals (if (= local-hostname hostname)
                      (multiget service)
                      (with-elephant-connection hostname port remote-service
                        (multiget remote-service)))]
      (map (fn [m v] (assoc m :value v))
           indexed-keys
           vals))))

(defprotocol Preparable
  (prepare [_] "Perform preparatory steps."))

(defn service-handler
  "Entry point to edb. `service-handler` returns a proxied
  implementation of EDB's interface."
  [edb-config]
  (let [^ReentrantReadWriteLock rw-lock (u/mk-rw-lock)
        {domain-map :domains :as database} (db/build-database edb-config)
        throttle  (dom/throttle (:download-rate-limit database))
        localhost (u/local-hostname)]
    (reify
      Preparable
      (prepare [this]
        (with-ret true
          (future
            (db/purge-unused-domains! database)
            (doseq [domain (vals domain-map)]
              (dom/boot-domain! domain rw-lock)))))
        
      Shutdownable
      (shutdown [_]
        (log/info "ElephantDB received shutdown notice...")
        (u/with-write-lock rw-lock
          (doseq [domain (vals domain-map)]
            (shutdown domain))))

      ElephantDB$Iface
      (directMultiGet [_ domain-name keys]
        (u/with-read-lock rw-lock
          (let [domain (domain-get database domain-name)]
            (u/dofor [key keys, :let [shard (dom/retrieve-shard domain key)]]
                     (log/debug
                      (format "Direct get: key %s at shard %s" key shard))
                     (if shard
                       (thrift/mk-value (.get shard key))
                       (throw (thrift/wrong-host-ex)))))))

      ;; Start out by indexing each key; this requires indexing each
      ;; key into a map (see `index-keys` above). The loop first
      ;; checks that every key has at least one host associated with
      ;; it. If any key is lacking hosts, the multiGet throws an
      ;; exception.
      ;;
      ;; If no exception is thrown, the system groups keys by the
      ;; first host in the list (localhost, if any keys are located on
      ;; the machine executing the call) and performs a directMultiGet
      ;; on each for its keys.
      ;;
      ;; If any host has unsuccessful results (didn't return
      ;; anything), the host is removed from the host lists of every
      ;; key, and multiGet recurses.
      ;;
      ;; Once the multi-get loop completes without any failures the
      ;; entire sequence of values is returned in order.
      (multiGet [this domain-name key-seq]
        (loop [indexed-keys (-> (domain-get database domain-name)
                                (index-keys key-seq))
               results []]
          (if-let [bad-key (some (comp empty? :hosts) indexed-keys)]
            (throw (thrift/hosts-down-ex (:all-hosts bad-key)))
            (let [host-map   (group-by (comp first :hosts) indexed-keys)
                  get-fn     (fn [host indexed-keys]
                               (multi-get* this domain-name localhost
                                           host port
                                           indexed-keys))
                  rets       (u/do-pmap (fn [[host indexed-keys]]
                                          [host (get-fn host indexed-keys)])
                                        host-map)
                  successful (into {} (filter second rets))
                  results    (->> (vals succeeded)
                                  (apply concat results))
                  fail-map   (apply dissoc host-map (keys succeeded))]
              (if (empty? failed-host-map)
                (map :value (sort-by :index results))
                (recur (map (fn [m]
                              (update-in m [:hosts] trim-hosts (keys fail-map)))
                            (apply concat (vals fail-map)))
                       results))))))
        
      (multiGetInt [this domain keys]
        (.multiGet this domain keys))

      (multiGetLong [this domain keys]
        (.multiGet this domain keys))

      (multiGetString [this domain keys]
        (.multiGet this domain keys))

      (get [this domain key]
        (first (.multiGet this domain [key])))

      (getInt [this domain key]
        (.get this domain key))

      (getLong [this domain key]
        (.get this domain key))

      (getString [this domain key]
        (.get this domain key))
        
      (getDomainStatus [_ domain-name]
        "Returns the thrift status of the supplied domain-name."
        (stat/status
         (domain-get database domain-name)))

      (getDomains [_]
        "Returns a sequence of all domain names being served."
        (keys domain-map))

      (getStatus [_]
        "Returns a map of domain-name->status for each domain."
        (thrift/elephant-status
         (u/val-map stat/status domain-map)))

      (isFullyLoaded [_]
        "Are all domains loaded properly?"
        (every? (some-fn stat/ready? stat/failed?)
                (vals domain-map)))

      (isUpdating [_]
        "Is some domain currently updating?"
        (let [domains (vals domain-map)]
          (some stat/loading? (map stat/status domains))))

      (update [_ domain-name]
        "If an update is available, updates the named domain and
         hotswaps the new version."
        (with-ret true
          (future
            (-> database
                (domain-get domain-name)
                (dom/attempt-update! rw-lock :throttle throttle)))))

      (updateAll [_]
        "If an update is available on any domain, updates the domain's
         shards from its remote store and hotswaps in the new versions."
        (with-ret true
          (future
            (do-pmap #(dom/attempt-update! % rw-lock :throttle throttle)
                     (vals domain-map))))))))

(defn thrift-server
  [service-handler port]
  (let [options (THsHaServer$Options.)]
    (set! (.maxWorkerThreads options) 64)
    (THsHaServer. (ElephantDB$Processor. service-handler)
                  (TNonblockingServerSocket. port)
                  (TBinaryProtocol$Factory.)
                  options)))

(defn launch-updater!
  [^ElephantDB$Iface service-handler interval-ms]
  (let [interval-ms (* 1000 interval-secs)]
    (future
      (log/info (format "Starting updater process with an interval of: %s seconds."
                        interval-secs))
      (while true
        (Thread/sleep interval-ms)
        (log/info "Updater process: firing update on all domains.")
        (.updateAll service-handler)))))
