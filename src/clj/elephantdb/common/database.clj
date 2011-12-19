(ns elephantdb.common.database
  (:require [clojure.string :as s]
            [hadoop-util.core :as h]
            [elephantdb.common.util :as u]
            [elephantdb.common.logging :as log]))

(defn purge-unused-domains!
  "Walks through the supplied local directory, recursively deleting
   all directories with names that aren't present in the supplied
   `domains`."
  [local-root domain-seq]
  (letfn [(domain? [path]
            (and (.isDirectory path)
                 (not (contains? (set domain-seq)
                                 (.getName path)))))]
    (u/dofor [domain-path (-> local-root h/mk-local-path .listFiles)
              :when (domain? domain-path)]
             (log/info "Destroying un-served domain at: " domain-path)
             (h/delete (h/local-filesystem)
                       (.getPath domain-path)
                       true))))

(defn prepare-local-domains!
  "Wipe domains not being used, make ready all cached domains, and get
  the downloading process started for all others."
  [domains-info edb-config rw-lock])

