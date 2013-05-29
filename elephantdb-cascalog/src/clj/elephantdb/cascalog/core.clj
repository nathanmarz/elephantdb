(ns elephantdb.cascalog.core
  (:use cascalog.api
        [elephantdb.cascalog conf])
  (:require [cascalog.workflow :as w])
  (:import [elephantdb Utils]
           [elephantdb.cascading ElephantDBTap ElephantDBTap$TapMode]
           [org.apache.hadoop.conf Configuration]))

(defn elephant-tap
  [root-path & {:keys [spec sink-fn ignore-spec] :as args}]
  (let [args (convert-args args)
        spec (when spec
               (convert-domain-spec spec))
        ignore-spec (and (or spec false) (or ignore-spec false))
        source-tap (ElephantDBTap. root-path spec args ElephantDBTap$TapMode/SOURCE ignore-spec)
        sink-tap (ElephantDBTap. root-path spec args ElephantDBTap$TapMode/SINK ignore-spec)]
    (cascalog-tap source-tap
                  (if sink-fn
                    (fn [tuple-src]
                      [sink-tap (sink-fn sink-tap tuple-src)])
                    sink-tap))))
