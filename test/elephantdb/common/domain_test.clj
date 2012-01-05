(ns elephantdb.common.domain-test
  (:use elephantdb.common.domain
        elephantdb.common.testing
        midje.sweet)
  (:require [hadoop-util.test :as t])
  (:import [elephantdb.store DomainStore]))

(def test-spec
  "Example spec for testing."
  (mk-test-spec 5))

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


;; ## Domain Type Testing

(fact ""
  (t/with-fs-tmp [_ tmp]
    (let [domain (build-domain tmp :spec test-spec)]
      )))
