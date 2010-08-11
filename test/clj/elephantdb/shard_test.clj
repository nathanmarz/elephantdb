(ns elephantdb.config-test
  (:use clojure.test)
  (:use [elephantdb shard testing]))

(defn run-shard-test [numhosts numshards replication]
  ;TODO finish
  )

(deftest test-sharder
  (run-shard-test 1 1 1)
  (run-shard-test 1 10 1)
  (run-shard-test 2 2 1)
  (run-shard-test 2 10 2)
  (run-shard-test 20 64 3)
  )
