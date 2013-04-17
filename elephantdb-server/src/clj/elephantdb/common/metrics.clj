(ns elephantdb.common.metrics
  (:import [com.yammer.metrics.reporting GraphiteReporter GangliaReporter]
           [java.util.concurrent TimeUnit]))

(defn report-to-graphite
  ([host port]
     (report-to-graphite 1 TimeUnit/MINUTES host port))
  ([period unit host port]
     (GraphiteReporter/enable period unit host port)))

(defn report-to-ganglia
  ([host port]
     (report-to-graphite 1 TimeUnit/MINUTES host port))
  ([period unit host port]
     (GangliaReporter/enable period unit host port)))
