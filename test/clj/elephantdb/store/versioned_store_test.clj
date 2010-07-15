(ns elephantdb.store.versioned-store-test
  (:use [clojure test])
  (:use [elephantdb testing hadoop util])
  (:import [elephantdb.store VersionedStore]))

(defmacro defvstest [name [vs-sym] & body]
  `(deffstest ~name [fs# dir#]
    (let [~vs-sym (VersionedStore. fs# dir#)]
      ~@body
      )))

(defvstest test-empty-version [vs]
  (let [v (.createVersion vs)]
    (.succeedVersion vs v)
    (is (= 1 (count (.getAllVersions vs))))
    (is (= v (.mostRecentVersionPath vs)))
    ))

(defvstest test-multiple-versions [vs]
  (.succeedVersion vs (.createVersion vs))
  (Thread/sleep 100)
  (let [v (.createVersion vs)]
    (.succeedVersion vs v)
    (is (= 2 (count (.getAllVersions vs))))
    (is (= v (.mostRecentVersionPath vs)))
    (Thread/sleep 100)
    (.createVersion vs)
    (is (= v (.mostRecentVersionPath vs)))
    ))

(defvstest test-error [vs]
  )

(defvstest test-cleanup [vs]
  )