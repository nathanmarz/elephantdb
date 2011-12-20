(ns elephantdb.common.testing
  (:use clojure.test)
  (:require [hadoop-util.core :as h]
            [jackknife.logging :as log])
  (:import [java.util UUID]
           [elephantdb ByteArray]))

;; TODO: Most of this code is now located inside of
;; `hadoop-util.test`. Move it out for good?

(defn uuid []
  (str (UUID/randomUUID)))

;; TODO: Add (when vals ,,,)
(defn barr [& vals]
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
