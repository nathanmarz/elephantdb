(ns elephantdb.common.database-test
  (:use elephantdb.common.database
        elephantdb.test.common
        midje.sweet)
  (:require [hadoop-util.test :as t]
            [elephantdb.common.domain :as domain]
            [elephantdb.common.status :as status])
  (:import [elephantdb.document KeyValDocument]))

(defn uuid-stream
  "Generates an infinite stream of UUIDs."
  []
  (repeatedly #(t/uuid)))

(defn build-test-db [root-dir remote-dir domain-seq]
  (let [spec (berkeley-spec 4)
        docs {0 [(KeyValDocument. 1 2)]
              1 [(KeyValDocument. 3 4) (KeyValDocument. 5 6)]}]
    (let [path-map (->> (uuid-stream)
                        (map (partial str remote-dir "/"))
                        (interleave domain-seq)
                        (apply hash-map))]
      (doseq [[domain-name remote-path] path-map]
        (create-domain! spec remote-path docs))
      (build-database {:local-root root-dir
                       :domains    path-map}))))

(defmacro with-database
  "Generates a database with the supplied sequence of domain names and
  binds it to `sym` inside of the form."
  [[sym domain-seq] & body]
  `(t/with-fs-tmp [fs# remote#]
     (t/with-local-tmp [lfs# local#]
       (let [~sym (build-test-db local# remote# ~domain-seq)]
         ~@body))))

(with-database [db ["domain-a"]]
  (facts "Domain-get should return nil when the domain doesn't exist."
    (domain-get db "random") => nil
    (domain-get db "domain-a") => domain/domain?
    (domain-names db) => ["domain-a"]

    "Nothing's been loaded yet."
    (fully-loaded? db) => false
    (some-loading? db) => false
    
    "We update the domain and wait until completion with a deref."
    @(attempt-update! db "domain-a")

    "Update's complete?"
    (some-loading? db) => false
    
    "Now the domain is fully loaded."
    (fully-loaded? db) => true

    "This is a half-baked test, but fine for now; we want to actually
    pass in data, not just check that the hardcoded data made it in
    all right."
    (count (seq (domain-get db "domain-a"))) => 3))
