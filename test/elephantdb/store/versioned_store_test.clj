(ns elephantdb.store.versioned-store-test
  (:import [elephantdb.store VersionedStore])
  (:use elephantdb.common.testing
        clojure.test)
  (:import [elephantdb.store VersionedStore]))

(defmacro def-vs-test [name [vs-sym] & body]
  `(def-fs-test ~name [fs# dir#]
     (let [~vs-sym (VersionedStore. fs# dir#)]
       ~@body)))

(def-vs-test test-empty-version [vs]
  (let [v (.createVersion vs)]
    (.succeedVersion vs v)
    (is (= 1 (count (.getAllVersions vs))))
    (is (= v (.mostRecentVersionPath vs)))))

(def-vs-test test-multiple-versions [vs]
  (.succeedVersion vs (.createVersion vs))
  (Thread/sleep 100)
  (let [v (.createVersion vs)]
    (.succeedVersion vs v)
    (is (= 2 (count (.getAllVersions vs))))
    (is (= v (.mostRecentVersionPath vs)))
    (Thread/sleep 100)
    (.createVersion vs)
    (is (= v (.mostRecentVersionPath vs)))))

(def-vs-test test-error [vs]
  )

(def-vs-test test-cleanup [vs]
  )
