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

(let [spec    (berkeley-spec 3)
      doc-seq [[0 (KeyValDocument. 1 2)]
               [0 (KeyValDocument. 3 4)]
               [1 (KeyValDocument. 5 6)]
               [1 (KeyValDocument. 7 8)]]]
 
  (with-basic-domain [domain spec doc-seq :version 10]
    (facts
      "Getting a seq on a domain should return the documents from ALL
       shards, no matter what the split."
      (seq domain) => (contains (map second doc-seq) :in-any-order)
      (current-version domain) => 10
      (newest-version domain) => 10
      (version-seq domain) => [10]
      (has-version? domain 5) => falsey
      (has-version? domain 10) => truthy
      (has-data? domain) => truthy))

  (t/with-fs-tmp [_ tmp]
    (let [domain (build-domain tmp :spec spec)]
      (facts
        "Testing an empty local domain."
        (version-seq domain) => nil
        (has-data? domain) => falsey
        (domain-data domain) => nil)

      "Start by creating 5 difference versions of a domain in the
      given temp file."
      (doseq [v (range 5)]
        (create-unsharded-domain! spec tmp doc-seq :version v))
      (facts (version-seq domain) => [4 3 2 1 0]
        (current-version domain) => nil
        (load-version! domain 1)
        (current-version domain) => 1
        (transfer-possible? domain 10) => false?))))
