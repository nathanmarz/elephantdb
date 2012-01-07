(ns elephantdb.common.database-test
  (:use elephantdb.common.database
        elephantdb.test.common
        midje.sweet)
  (:require [elephantdb.common.domain :as domain]
            [hadoop-util.test :as t])
  (:import [elephantdb.document KeyValDocument]))

(defn uuid-stream []
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

    "We update the domain and wait until completion with a deref."
    @(attempt-update! db "domain-a")

    "Now the domain is fully loaded."
    (fully-loaded? db) => true))
