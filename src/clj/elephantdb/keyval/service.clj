(ns elephantdb.keyval.service
  (:use [elephantdb.keyval.thrift :only (with-elephant-connection)])
  (:require [jackknife.core :as u]
            [jackknife.logging :as log]
            [elephantdb.common.domain :as dom]
            [elephantdb.common.hadoop :as hadoop]
            [elephantdb.common.database :as db]
            [elephantdb.common.thrift :as thrift])
  (:import [org.apache.thrift TException]
           [org.apache.thrift.server THsHaServer THsHaServer$Options]
           [org.apache.thrift.protocol TBinaryProtocol$Factory]
           [org.apache.thrift.transport TNonblockingServerSocket]
           [java.util.concurrent.locks ReentrantReadWriteLock]
           [elephantdb.persistence Shutdownable]
           [elephantdb.generated ElephantDB ElephantDB$Iface ElephantDB$Processor
            WrongHostException DomainNotFoundException DomainNotLoadedException]))

;; ## Service Handler
;;
;; This logic is not really even particular to key value -- the only
;;thing we need to know is how to access a given persistence, which we
;;can pass around in Clojure with a function!

(defn trim-hosts
    "Used within a multi-get's loop. Accepts a sequence of hosts + a
    sequence of hosts known to be bad, filters the bad hosts and drops
    the first one."
    [host-seq bad-hosts]
    (remove (set bad-hosts)
            (rest host-seq)))

(defn index-keys
  "returns {:index}[hosts-to-try global-index key all-hosts] seq"
  [domain keys]
  (for [[idx key] (map-indexed vector keys)
        :let [hosts (dom/prioritize-hosts domain key)]]
    {:index idx, :key key, :hosts hosts, :all-hosts hosts}))

(defn try-multi-get
  [service domain-name error-suffix key-seq]
  (let []
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
           (throw e)))))

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

(defprotocol IPreparable
  (prepare [_] "Perform preparatory steps."))

(defn service-handler
  "Entry point to edb. `service-handler` returns a proxied
  implementation of EDB's interface."
  [edb-config]
  (let [^ReentrantReadWriteLock rw-lock (u/mk-rw-lock)
        
        download-supervisor (atom nil)
        localhost (u/local-hostname)
        {domain-map :domains :as database} (db/build-database edb-config)
        throttle (hadoop/throttle (:download-cap database))]
    (reify
      IPreparable
      (prepare [this]
        (with-ret true
          (future
            (db/purge-unused-domains! database)
            (doseq [domain (vals domains)]
              (dom/boot-domain! domain rw-lock)))))
        
      Shutdownable
      (shutdown [_]
        (log/info "ElephantDB received shutdown notice...")
        (u/with-write-lock rw-lock
          (doseq [domain (vals domain-map)]
            (shutdown domain))))

      ElephantDB$Iface
      (get [this domain key]
        (first (.multiGet this domain [key])))

      (getInt [this domain key]
        (.get this domain key))

      (getLong [this domain key]
        (.get this domain key))

      (getString [this domain key]
        (.get this domain key))

      ;; TODO: We need to handle the interface between thrift and the
      ;; rest of the system.
      (directMultiGet [_ domain-name keys]
        (u/with-read-lock rw-lock
          (let [domain (db/domain-get database domain-name)]
            (u/dofor [key keys, :let [shard (dom/retrieve-shard domain key)]]
                     (log/debug
                      (format "Direct get: key %s at shard %s" key shard))
                     (if shard
                       (thrift/mk-value (.get shard key))
                       (throw (thrift/wrong-host-ex)))))))

      ;; Start out by indexing each key; this requires indexing each
      ;; key into a map (see `index-keys` above). The loop first
      ;; checks that every key has at least one host associated with
      ;; it. If any key is lacking hosts, multiGet throws an
      ;; exception for the entire multiGet.
      ;;
      ;; Assuming that doesn't happen, the system groups keys by the
      ;; first host in the list (localhost, if any keys are located
      ;; on the machine executing the call) and performs a
      ;; directMultiGet.
      ;;
      ;; If any host had unsuccessful results (didn't return
      ;; anything, basically), it's removed from the host lists of
      ;; every key for the subsequent loops.
      ;;
      ;; Once the multi-get loop completes without any failures the
      ;; entire sequence of keys is returned.
      (multiGet [this domain-name key-seq]
        (loop [indexed-keys (-> (db/domain-get database domain-name)
                                (index-keys key-seq))
               results []]
          (if-let [bad-key (some (comp empty? :hosts) indexed-keys)]
            (throw (thrift/hosts-down-ex (:all-hosts bad-key)))
            (let [host-map   (group-by (comp first :hosts) indexed-keys)
                  get-fn     (fn [host indexed-keys]
                               (multi-get* this domain-name
                                           localhost host
                                           port indexed-keys))
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
                            (apply concat (vals failed-host-map)))
                       results))))))
        
      (multiGetInt [this domain keys]
        (.multiGet this domain keys))

      (multiGetLong [this domain keys]
        (.multiGet this domain keys))

      (multiGetString [this domain keys]
        (.multiGet this domain keys))
        
      (getDomainStatus [_ domain-name]
        (stat/status
         (db/domain-get database domain-name)))

      (getDomains [_] (keys domain-map))

      (getStatus [_]
        (thrift/elephant-status
         (u/val-map stat/status domain-map)))

      (isFullyLoaded [this]
        "Are all domains loaded properly?"
        (every? (some-fn stat/ready? stat/failed?)
                (vals domain-map)))

      (isUpdating [this]
        "Is some domain currently updating?"
        (let [domains (vals domain-map)]
          (some stat/loading? (map stat/status domains))))

      (update [this domain-name]
        (with-ret true
          (future
            (-> (db/domain-get domain-name)
                (dom/attempt-update! rw-lock :throttle throttle)))))

      (updateAll [this]
        (with-ret true
          (future
            (do-pmap #(dom/attempt-update! % rw-lock :throttle throttle)
                     (vals domains))))))))

(defn thrift-server
  [service-handler port]
  (let [options (THsHaServer$Options.)]
    (set! (.maxWorkerThreads options) 64)
    (THsHaServer. (ElephantDB$Processor. service-handler)
                  (TNonblockingServerSocket. port)
                  (TBinaryProtocol$Factory.)
                  options)))

(defn launch-updater!
  "Returns a future."
  [^ElephantDB$Iface service-handler interval-ms]
  (let [interval-ms (* 1000 interval-secs)]
    (future
      (log/info (format "Starting updater process with an interval of: %s seconds..."
                        interval-secs))
      (while true
        (Thread/sleep interval-ms)
        (log/info "Updater process: Checking if update is possible...")
        (.updateAll service-handler)))))
