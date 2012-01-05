(ns elephantdb.common.domain-test
  (:use elephantdb.common.domain
        elephantdb.test.common
        midje.sweet)
  (:require [hadoop-util.test :as t])
  (:import [elephantdb.store DomainStore]
           [elephantdb.document KeyValDocument]))

(def test-spec
  "BerkeleyDB-based DomainSpec for testing."
  (berkeley-spec 5))

(fact "Test try-domain-store."
  (t/with-fs-tmp [fs tmp]
   (fact "Nonexistent stores are falsey."
     (try-domain-store tmp) => falsey)

   "Then we create a domain store..."
   (DomainStore. tmp test-spec)
   (fact "And become truthy."
     (try-domain-store tmp) => truthy)))

(fact "test local store creation."
  (t/with-fs-tmp [fs local remote]
    (fact "Nonexistent remote store throws an exception."
      (mk-local-store local remote) => (throws RuntimeException))

    "Create the remote store..."
    (let [remote-store (DomainStore. remote test-spec)
          local-store  (mk-local-store local remote)]
      (fact "Now the specs should be equal."
        (.getSpec local-store) => (.getSpec remote-store)))))

(fact
  "Getting a seq on a domain should return the documents from ALL
   shards, no matter what the split."
  (let [shard-seq [0 0 1 1]
        doc-seq   [(KeyValDocument. 1 2)
                   (KeyValDocument. 3 4)
                   (KeyValDocument. 5 6)
                   (KeyValDocument. 7 8)]]
    (with-basic-domain [domain
                        (berkeley-spec 3)
                        (map vector shard-seq doc-seq)]
      (seq domain) => (contains doc-seq :in-any-order))))
