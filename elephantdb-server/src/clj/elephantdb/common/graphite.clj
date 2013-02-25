(ns elephantdb.common.graphite
  (:import [com.yammer.metrics.reporting GraphiteReporter]
           [java.util.concurrent TimeUnit]))

(defn report-to-graphite
  ([host port]
     (report-to-graphite 1 TimeUnit/MINUTES host port))
  ([period unit host port]
     (GraphiteReporter/enable period unit host port)))