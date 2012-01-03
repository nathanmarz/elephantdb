(ns elephantdb.store.versioned-store-test
  (:use elephantdb.common.testing
        midje.sweet)
  (:import [elephantdb.store VersionedStore]))

(defmacro def-vs-test [name [vs-sym] & body]
  `(def-fs-test ~name [fs# dir#]
     (let [~vs-sym (VersionedStore. fs# dir#)]
       ~@body)))

(def-vs-test test-empty-version [vs]
  (let [v (.createVersion vs)]
    (.succeedVersion vs v)
    (facts
      (count (.getAllVersions vs)) => 1
      (.mostRecentVersionPath vs)  => v)))

(def-vs-test test-multiple-versions [vs]
  (.succeedVersion vs (.createVersion vs))
  (Thread/sleep 100)
  (let [v (.createVersion vs)]
    (.succeedVersion vs v)
    (facts
      (count (.getAllVersions vs)) => 2
      (.mostRecentVersionPath vs)  => v)
    (Thread/sleep 100)
    (.createVersion vs)
    (fact
      (.mostRecentVersionPath vs)) => v))

(def-vs-test test-error [vs]
  )

(def-vs-test test-cleanup [vs]
  )
