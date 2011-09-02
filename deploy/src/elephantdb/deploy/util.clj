(ns elephantdb.deploy.util
  (:require [clojure.string :as s]
            [pallet.execute :as execute]))

(def env-keys-to-resolve [:public-key-path :private-key-path])

(defn resolve-path [path]
  (s/trim (:out (execute/local-script (echo ~path)))))

(defn resolve-keypaths
  [user-map]
  (reduce #(%2 %1)
          user-map
          (for [kwd env-keys-to-resolve]
            #(if (kwd %)
               (update-in % [kwd] resolve-path)
               %))))
