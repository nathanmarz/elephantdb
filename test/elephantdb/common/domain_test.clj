(ns elephantdb.common.domain-test
  (:use elephantdb.common.domain
        elephantdb.common.testing
        midje.sweet)
  (:import [elephantdb.store DomainStore]))

(def test-spec
  (mk-test-spec 5))

(fact
  (with-fs-tmp [fs local remote]
    (let [remote-store (DomainStore. remote test-spec)]
      )))
