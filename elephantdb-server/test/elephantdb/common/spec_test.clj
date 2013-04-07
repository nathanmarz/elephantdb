(ns elephantdb.common.spec-test
  (:use midje.sweet
        [elephantdb.test.common :only (berkeley-spec)])
  (:import [elephantdb DomainSpec Utils]
           [elephantdb.document KeyValDocument]))

;; ## DomainSpec Testing

(fact
  "DomainSpec equality is value-based, not instance-based."
  (DomainSpec. "elephantdb.persistence.JavaBerkDB"
               "elephantdb.partition.HashModScheme"
               2)
  => (DomainSpec. (elephantdb.persistence.JavaBerkDB.)
                  (elephantdb.partition.HashModScheme.)
                  2))

;; ## DomainSpec functionality

(fact
  "Spec should only allow positive numbers for the shard-count."
  (berkeley-spec 10)  => truthy
  (berkeley-spec 0)   => (throws AssertionError)
  (berkeley-spec -10) => (throws AssertionError))
