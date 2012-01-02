(ns elephantdb.keyval.main
  (:use elephantdb.common.config)
  (:require [jackknife.core :as u]
            [jackknife.logging :as log]
            [elephantdb.common.database :as db]
            [elephantdb.common.thrift :as thrift])
  (:gen-class))

