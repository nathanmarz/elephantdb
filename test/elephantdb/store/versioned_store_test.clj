(ns elephantdb.store.versioned-store-test
  (:use elephantdb.common.testing
        midje.sweet)
  (:import [elephantdb.store VersionedStore]))

(defmacro with-versioned-store [[sym] & body]
  `(with-fs-tmp [fs# dir#]
     (let [~sym (VersionedStore. fs# dir#)]
       ~@body)))

(fact "Testing the empty versions."
  (with-versioned-store [vs]
    (let [v (.createVersion vs)]
      (.succeedVersion vs v)
      (facts
        (count (.getAllVersions vs)) => 1
        (.mostRecentVersionPath vs)  => v))))

(fact "Multiple versions should resolve properly."
  (with-versioned-store [vs]
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
        (.mostRecentVersionPath vs) => v))))

(future-fact
 "Versioned store error testing.")

(future-fact
 "Versioned store should cleanup properly if a non-succeeded version
  is created..")

