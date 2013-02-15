(ns elephantdb.common.database-test
  (:use elephantdb.common.database
        midje.sweet)
  (:require [elephantdb.test.common :as t]
            [elephantdb.common.domain :as domain])
  (:import [elephantdb.document KeyValDocument]))

(defn count-equals [n]
  (chatty-checker [coll] (= n (count (seq coll)))))

(t/with-database [db {"domain-a" {0 [(KeyValDocument. (t/str->barr "foo") (t/str->barr "bar"))]
                                  1 [(KeyValDocument. (t/str->barr "lol") (t/str->barr "cat")) (KeyValDocument. (t/str->barr "oh") (t/str->barr "hai"))]}}]
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
    (domain-get db "domain-a") => (count-equals 3)))
