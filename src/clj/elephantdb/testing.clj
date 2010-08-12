(ns elephantdb.testing
  (:import [java.util UUID ArrayList])
  (:import [elephantdb Utils ByteArray])
  (:import [elephantdb.hadoop ElephantRecordWritable ElephantOutputFormat
            ElephantOutputFormat$Args])
  (:import [elephantdb.store VersionedStore])
  (:import [org.apache.hadoop.io IntWritable])
  (:import [org.apache.hadoop.mapred JobConf])
  (:use [elephantdb util hadoop config shard service])
  (:use [clojure test]))

(defn uuid []
  (str (UUID/randomUUID)))

(defn delete-all [fs paths]
  (dorun
    (for [t paths]
      (.delete fs (path t) true))))

(defmacro with-fs-tmp [[fs-sym & tmp-syms] & body]
  (let [tmp-paths (mapcat (fn [t] [t '(str "/tmp/unittests/" (uuid))]) tmp-syms)]
    `(let [~fs-sym (filesystem)
           ~@tmp-paths]
       (.mkdirs ~fs-sym (path "/tmp/unittests"))
        (try
          ~@body
          (finally
         (delete-all ~fs-sym ~(vec tmp-syms)))
        ))))

(defmacro deffstest [name fs-args & body]
  `(deftest ~name
      (with-fs-tmp ~fs-args
        ~@body )))

(defn local-temp-path []
  (str (System/getProperty "java.io.tmpdir") "/" (uuid)))

(defmacro with-local-tmp [[fs-sym & tmp-syms] & body]
  (let [tmp-paths (mapcat (fn [t] [t `(local-temp-path)]) tmp-syms)]
    `(let [~fs-sym (local-filesystem)
           ~@tmp-paths]
      (try
        ~@body
      (finally
       (delete-all ~fs-sym ~(vec tmp-syms)))
      ))
    ))

(defmacro deflocalfstest [name local-args & body]
  `(deftest ~name
      (with-local-tmp ~local-args
        ~@body )))

(defn add-string [db key value]
  (.add db (.getBytes key) (.getBytes value)))

(defn get-string [db key]
  (if-let [r (.get db (.getBytes key))]
    (String. r)
    nil ))


;bind this to get different behavior when making sharded domains
(defn test-key-to-shard [key numshards]
  (Utils/keyShard key numshards)
  )

(defn mk-elephant-writer [shards factory output-dir tmpdir & kargs]
  (let [kargs (apply hash-map kargs)
        args (ElephantOutputFormat$Args.
              (convert-clj-domain-spec
               {:num-shards shards
                :persistence-factory factory})
              output-dir)
        ]
    (.setTmpDirs args (ArrayList. [tmpdir]))
    (if-let [upd (:updater kargs)]
      (set! (. args updater) upd))
    (if-let [update-dir (:update-dir kargs)]
      (set! (. args updateDirHdfs) update-dir))
    (.getRecordWriter (ElephantOutputFormat.)
                      nil
                      (doto
                          (JobConf.)
                        (Utils/setObject
                         ElephantOutputFormat/ARGS_CONF
                         args ))
                      nil
                      nil )))

(defn mk-sharded-domain [fs path domain-spec keyvals]
  (with-local-tmp [lfs localtmp]
    (let [vs (VersionedStore. fs path)
          dpath (.createVersion vs)
          shardedkeyvals (map
                          (fn [[k v]]
                            [(test-key-to-shard k (:num-shards domain-spec))
                             k
                             v])
                          keyvals)
          writer (mk-elephant-writer
                  (:num-shards domain-spec)
                  (:persistence-factory domain-spec)
                  dpath
                  localtmp)]
      (write-domain-spec! domain-spec fs path)
      (doseq [[s k v] shardedkeyvals]
        (when v
          (.write writer
                  (IntWritable. s)
                  (ElephantRecordWritable. k v))))
      (.close writer nil)
      (.succeedVersion vs dpath)    
      )))

(defn reverse-pre-sharded [shardmap]
  (map-mapvals
   first
   (reverse-multimap
    (map-mapvals #(map (fn [x] (ByteArray. (first x))) %) shardmap))))

(defn mk-presharded-domain [fs path factory shardmap]
  (let [keyvals (apply concat (vals shardmap))
        shards (reverse-pre-sharded shardmap)
        domain-spec {:num-shards (count shardmap) :persistence-factory factory}
        ]
    (binding [test-key-to-shard (fn [k _] (shards (ByteArray. k)))]
      (mk-sharded-domain fs path domain-spec keyvals))
    ))

(defn mk-service-handler [global-config localdir token domain-to-host-to-shards]
  (binding [compute-host-to-shards (if domain-to-host-to-shards
                                     (fn [d _ _ _] (domain-to-host-to-shards d))
                                     compute-host-to-shards)]
    (let [handler (service-handler global-config {:local-dir localdir} token)]
      (while (not (.isFullyLoaded handler))
        (println "waiting...")
        (Thread/sleep 500))
      handler )))

(defmacro with-sharded-domain [[pathsym domain-spec keyvals] & body]
  `(with-fs-tmp [fs# ~pathsym]
     (mk-sharded-domain fs# ~pathsym ~domain-spec ~keyvals)
     ~@body
     ))

(defmacro with-presharded-domain [[dname pathsym factory shardmap] & body]
  `(with-fs-tmp [fs# ~pathsym]
     (mk-presharded-domain
      fs#
      ~pathsym
      ~factory
      ~shardmap)
     (binding [key-shard (let [prev# key-shard
                               rev# (reverse-pre-sharded ~shardmap)]
                           (fn [d# k# a#]
                             (if (= d# ~dname)
                               (rev# (ByteArray. k#))
                               (prev# d# k# a#)
                               )))]
       ~@body)
     ))

(defmacro with-service-handler
  [[handler-sym hosts domains-conf domain-to-host-to-shards] & body]
  (let [global-conf {:replication 1 :hosts hosts :domains domains-conf}]
    `(with-local-tmp [lfs# localtmp#]
       (let [~handler-sym (mk-service-handler ~global-conf localtmp# (System/currentTimeMillis) ~domain-to-host-to-shards)]
         ~@body
         ))))

(defmacro with-single-service-handler
  [[handler-sym domains-conf] & body]
  `(with-service-handler [~handler-sym [(local-hostname)] ~domains-conf nil]
     ~@body
     ))

(defn check-domain-pred [domain-name handler pairs pred]
  (doseq [[k v] pairs]
    (let [newv (-> handler (.get domain-name k) (.get_data))]
      (is (pred v newv) (str (seq k) (seq v) (seq newv)))
      )))

(defn barr [& vals]
  (byte-array (map byte vals)))

(defn barr= [& vals]
  (apply = (map #(ByteArray. %) vals)))

(defn check-domain [domain-name handler pairs]
  (check-domain-pred domain-name handler pairs barr=))

(defn check-domain-not [domain-name handler pairs]
  (check-domain-pred domain-name handler pairs (complement barr=)))
