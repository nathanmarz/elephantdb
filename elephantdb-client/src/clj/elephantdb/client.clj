(ns elephantdb.client
  (:refer-clojure :exclude (get))
  (:import [java.nio ByteBuffer]
           [org.apache.thrift TSerializer]
           [org.apache.thrift.protocol TBinaryProtocol]
           [org.apache.thrift.transport TFramedTransport TSocket]
           [org.apache.thrift TException]
           [elephantdb.generated DomainNotFoundException
            DomainNotLoadedException WrongHostException Value]
           [elephantdb.generated.keyval ElephantDB$Client]))

;; ## Utility functions

(defn- test-array
  [t]
  (let [check (type (t []))]
    (fn [arg] (instance? check arg))))

(def ^{:private true} byte-array?
  (test-array byte-array))

(defn bytes->bytebuffer
  "Wraps a collection of byte arrays in ByteBuffers."
  [coll]
  (map (fn [^bytes x]
         {:pre [(byte-array? x)]}
         (ByteBuffer/wrap x)) coll))

(defn- parse-results-map
  "Transform the results map returned from a call
   back into a map of key and value byte arrays."
  [m]
  (into {} (for [[k v] m
                 :let [ret (byte-array (.remaining k))]
                 :when (.get k ret)]
             [ret (.get_data v)])))

(defn kv-client [transport]
  (ElephantDB$Client. (TBinaryProtocol. transport)))

(defn thrift-transport
  [host port]
  (TFramedTransport. (TSocket. host port)))

;; ## Client Interface functions.

(defmacro with-elephant
  "Calls to ElephantDB should be wrapped in this macro
   which handles the thrift connection boiler plate."
  [host port client-sym & body]
  `(with-open [^TFramedTransport conn# (doto ^TFramedTransport (thrift-transport ~host ~port)
                                         (.open))]
     (let [^ElephantDB$Client ~client-sym (kv-client conn#)]
       ~@body)))

(defn get
  "Makes a `get` call to ElephantDB and unwraps the thrfit Value struct,
   returing a byte-array representing the value or nil."
  [connection domain ^bytes key]
  (let [key (ByteBuffer/wrap key)]
    (when-let [^Value value (.get connection domain key)]
      (.get_data value))))

(defn multi-get
  "Makes a `multi-get` call to ElephantDB. The result map is transformed
   into a map of key and value byte array pairs."
  [connection domain key-seq]
  (let [key-set (into #{} (bytes->bytebuffer key-seq))]
    (when-let [results-map (.multiGet connection domain key-set)]
      (parse-results-map results-map))))

(defn get-thrift
  "A convience wrapper around get for use with a thrift-based key."
  [connection domain key]
  (let [s (TSerializer.)
        key (.serialize s key)]
    (get connection domain key)))

(defn multi-get-thrift
  "A convience wrapper around multi-get for use with thrift-based keys."
  [connection domain key-seq]
  (let [s (TSerializer.)
        key-seq (map #(.serialize s %) key-seq)]
    (multi-get connection domain key-seq)))

(defn get-domains
  "Get a list of domains available."
  [connection]
  (.getDomains connection))

(defn get-status
  "Get the status of all domains."
  [connection]
  (.getStatus connection))

(defn get-domain-status
  "Get the status of the supplied domain."
  [connection domain]
  (.getDomainStatus connection domain))

(defn fully-loaded?
  "Check if all domains are fully loaded."
  [connection]
  (.isFullyLoaded connection))

(defn updating?
  "Are any domains currently updating."
  [connection]
  (.isUpdating connection))

(defn update
  "If an update is available, updates the named domain
  and hotswap the new version."
  [connection domain]
  (.update connection domain))

(defn update-all
  "If an update is available on any domain, updates the domain's
  and hotswaps in the new versions."
  [connection]
  (.updateAll connection))

(defn get-domain-metadata
  "Get metadata for the supplied domain."
  [connection domain]
  (.getDomainMetaData connection domain))

(defn get-metadata
  "Get metadata for all domains."
  [connection]
  (.getMetaData connection))
