(ns elephantdb.testing
  (:import [java.util UUID])
  (:use [elephantdb util hadoop])
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
