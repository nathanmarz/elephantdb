(ns elephantdb.common.testing
  (:use clojure.test)
  (:require [hadoop-util.core :as h]
            [jackknife.logging :as log]
            [elephantdb.common.domain :as dom])
  (:import [java.util UUID]
           [elephantdb.store DomainStore]
           [elephantdb ByteArray]))

;; ## Domain Testing

(defn specs-match?
  "Returns true of the specs of all supplied DomainStores match, false
  otherwise."
  [& stores]
  (apply = (map (fn [^DomainStore x]
                  (.getSpec x))
                stores)))

;; `existing-shard-set` is good for testing, but we don't really need
;; it in a working production system (since the domain knows what
;; shards should be holding.)

(defn existing-shard-set
  "Returns a sequence of all shards present on the fileystem for the
  supplied store and version. Useful for testing."
  [store version]
  (let [num-shards (.. store getSpec getNumShards)
        filesystem (.getFileSystem store)]
    (->> (range num-shards)
         (filter (fn [idx]
                   (->> (.shardPath store idx version)
                        (.exists filesystem))))
         (into #{}))))

(defn version-well-formed?
  [domain version]
  (= (dom/shard-set domain version)
     (-> (:local-store domain)
         (existing-shard-set version))))

;; ## Byte Array Testing

(defn barr
  "TODO: Add (when vals ,,,)"
  [& vals]
  (byte-array (map byte vals)))

(defn barr=
  [& vals]
  (apply = (map #(ByteArray. %) vals)))

(defn barrs= [& arrs]
  (and (apply = (map count arrs))
       (every? identity
               (apply map (fn [& vals]
                            (or (every? nil? vals)
                                (apply barr= vals)))
                      arrs))))

;; ## Hadoop Testing Utilities
;;
;; TODO: Most of this code is now located inside of
;; `hadoop-util.test`. Move it out for good.

(defn uuid []
  (str (UUID/randomUUID)))

;; TODO: Move to hadoop-util.
(defn delete-all [fs paths]
  (dorun
   (for [t paths]
     (.delete fs (h/path t) true))))

(defmacro with-fs-tmp
  [[fs-sym & tmp-syms] & body]
  (let [tmp-paths (mapcat (fn [t]
                            [t `(str "/tmp/unittests/" (uuid))])
                          tmp-syms)]
    `(let [~fs-sym (h/filesystem)
           ~@tmp-paths]
       (.mkdirs ~fs-sym (h/path "/tmp/unittests"))
       (try ~@body
            (finally
             (delete-all ~fs-sym [~@tmp-syms]))))))

(defmacro def-fs-test
  [name fs-args & body]
  `(deftest ~name
     (with-fs-tmp ~fs-args
       ~@body)))

(defn local-temp-path []
  (str (System/getProperty "java.io.tmpdir") "/" (uuid)))

(defmacro with-local-tmp
  [[fs-sym & tmp-syms] & [kw & more :as body]]
  (let [[log-lev body] (if (keyword? kw)
                         [kw more]
                         [:warn body])
        tmp-paths (mapcat (fn [t]
                            [t `(local-temp-path)])
                          tmp-syms)]
    `(log/with-log-level ~log-lev
       (let [~fs-sym (h/local-filesystem)
             ~@tmp-paths]
         (try ~@body
              (finally
               (delete-all ~fs-sym [~@tmp-syms])))))))

(defmacro def-local-fs-test [name local-args & body]
  `(deftest ~name
     (with-local-tmp ~local-args
       ~@body)))
