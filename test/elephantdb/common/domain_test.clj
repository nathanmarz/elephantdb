(ns elephantdb.common.domain-test
  (:use elephantdb.common.domain
        elephantdb.common.testing
        midje.sweet)
  (:import [elephantdb.store DomainStore]))


(fact
  (with-fs-tmp [_ local remote]
    (DomainStore. remote )
    ))
