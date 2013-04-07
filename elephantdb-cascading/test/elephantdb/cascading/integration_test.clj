(ns elephantdb.cascading.integration-test
  (:use midje.sweet)
  (:require [jackknife.logging :as log]
            [hadoop-util.test :as test]
            [clojure.string :as s])
  (:import [cascading.pipe Pipe]
           [cascading.tuple Fields Tuple]
           [cascading.flow.hadoop HadoopFlowProcess HadoopFlowConnector]
           [cascading.tap.hadoop Hfs]
           [elephantdb.partition HashModScheme]
           [elephantdb.persistence JavaBerkDB]
           [elephantdb DomainSpec]
           [elephantdb.document KeyValDocument]
           [elephantdb.cascading ElephantDBTap
            ElephantDBTap$Args ElephantDBTap$TapMode KeyValTailAssembly]
           [org.apache.hadoop.mapred JobConf]))

;; ## Global Vars

(def ^:dynamic *default-conf* {})

;; ## Key-Value

(defn kv-spec
  "Returns a DomainSpec initialized with a hash-mod sharding scheme, a
  key-value persistence and the supplied sharding count."
  [shard-count]
  (DomainSpec. (JavaBerkDB.)
               (HashModScheme.)
               shard-count))

(defn hfs-tap
  "Returns an Hfs tap with the default sequencefile scheme and the
  supplied fields."
  [path & fields]
  (-> (Fields. (into-array fields))
      (Hfs. path)))

(defn kv-tap
  "Returns an HFS SequenceFile tap with two fields for key and
  value."
  [path]
  (hfs-tap path "key" "value"))

(def defaults
  {"io.serializations"
   (s/join "," ["org.apache.hadoop.io.serializer.WritableSerialization"
                "cascading.tuple.hadoop.BytesSerialization"
                "cascading.tuple.hadoop.TupleSerialization"])})

(def mk-props
  (partial merge defaults))

(defn job-conf
  "Returns a JobConf instance, optionally augmented by the supplied
   property map."
  [& [prop-map]]
  (let [conf (JobConf.)]
    (doseq [[k v] (mk-props (or prop-map *default-conf*))]
      (.set conf k v))
    conf))

(defn flow-connector
  "Returns an instance of FlowConnection, optionally augmented by the
   supplied property map."
  [& [prop-map]]
  (HadoopFlowConnector. (mk-props (or prop-map *default-conf*))))

(defn conj-serialization!
  "Appends the supplied serialization to the supplied configuration
  map. object. Returns the modified JobConf object."
  [conf-map serialization]
  (merge-with #(str %1 "," %2)
              conf-map
              {"io.serializations"  serialization}))

(let [conf {"io.serializations" "StringWrapper"}]
  (fact
    (conj-serialization! {} "CakeWrapper") => {"io.serializations" "CakeWrapper"}
    "Serializations should join up with commas."
    (conj-serialization! conf "CakeWrapper") => {"io.serializations"
                                                 "StringWrapper,CakeWrapper"}))

(defn elephant-tap
  "Returns an ElephantDB Tap tuned to the supplied path and
  shard-count. Optionally, you can supply keyword arguments as
  specified by `kv-opts`."
  [path shard-count tap-mode]
  (ElephantDBTap. path
                  (kv-spec shard-count)
                  (ElephantDBTap$Args.)
                  tap-mode))

(defn connect!
  "Connect the supplied source and sink with the supplied pipe."
  [pipe source sink]
  (doto (.connect (flow-connector) source sink pipe)
    (.complete)))

(defn tuple-seq
  "Returns all tuples in the supplied cascading tap as a Clojure
  sequence."
  [sink]
  (with-open [it (-> (HadoopFlowProcess. (job-conf))
                     (.openTapForRead sink))]
    (doall (for [wrapper (iterator-seq it)]
             (into [] (.getTuple wrapper))))))

;; ## Transfer Functions

(defn elephant->hfs!
  "Transfers all tuples from the supplied elephant-tap into the
  supplied cascading `sink`."
  [elephant-source sink]
  (connect! (Pipe. "pipe")
            elephant-source
            sink))

(defn hfs->elephant!
  "Transfers all tuples from the supplied cascading `source` to the
  supplied elephant-tap."
  [source elephant-sink]
  (connect! (-> (Pipe. "pipe")
                (KeyValTailAssembly. elephant-sink))
            source
            elephant-sink))

(defn populate!
  "Accepts a SequenceFile tap, a sequence of key-value pairs (and an
  optional JobConf instance, supplied with the :conf keyword argument)
  and sinks all key-value pairs into the tap. Returns the original tap
  instance.."
  [kv-tap tuples]
  (with-open [collector (-> (HadoopFlowProcess. (job-conf))
                            (.openTapForWrite kv-tap))]
    (doseq [tuple tuples]
      (.add collector (Tuple. (into-array Object tuple)))))
  kv-tap)

(defn populate-edb!
  "Fills the supplied elephant-sink with the the supplied sequence of
  kv-pairs."
  [elephant-sink pairs]
  (test/with-fs-tmp [_ tmp]
    (-> (kv-tap tmp)
        (populate! pairs)
        (hfs->elephant! elephant-sink))))

(defn produces
  "Returns a chatty checker that tests for equality between two
  sequences of tuples. Accepts an optional property map as the second
  parameter."
  [expected]
  (chatty-checker [actual]
                  (= (set expected)
                     (set (tuple-seq actual)))))

(defmacro with-kv-tap
  "Accepts a binding vector with the tap-symbol (for binding, as with
  `let`), the shard-count and optional keyword arguments to be passed
  on to `kv-opts` above.

  `with-kv-tap` accepts a `:log-level` optional keyword argument that
  can be used to tune the output of all jobs run within the
  form. Valid log level values or `:fatal`, `:warn`, `:info`, `:debug`
  and `:off`.

  To change the configuration map used for the test, supply a map
  using the `:conf` keyword argument."
  [[sym shard-count tap-mode & opts] & body]
  (let [opt-map   (apply hash-map opts)
        log-level (:log-level opt-map :off)
        conf      (:conf opt-map *default-conf*)]
    `(binding [*default-conf* (or ~conf {})]
       (log/with-log-level ~log-level
         (test/with-fs-tmp [fs# tmp#]
           (let [~sym (elephant-tap tmp# ~shard-count ~tap-mode)]
             ~@body))))))

;; ## Tests

(tabular
 (future-fact
   "Tuples sunk into an ElephantDB tap and read back out should
    match. (A map acts as a sequence of 2-tuples, perfect for
    ElephantDB key-val tests.)"
   (with-kv-tap [e-tap 4 ElephantDBTap$TapMode/SINK]
     (populate-edb! e-tap ?tuples)
     e-tap => (produces ?tuples)))
 ?tuples
 {"key" "val", "ham" "burger"})
