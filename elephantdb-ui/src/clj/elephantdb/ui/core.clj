(ns elephantdb.ui.core
  (:use ring.adapter.jetty)
  (:require [elephantdb.ui.handler :as handler]
            [carica.core :refer [configurer
                                 resources]])
  (:gen-class))

(def config (configurer (resources "ui_config.clj")))

(defn -main []
  (run-jetty handler/app {:port (config :ui-port)}))
