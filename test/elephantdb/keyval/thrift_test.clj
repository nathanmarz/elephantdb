(ns elephantdb.keyval.thrift-test
  (:use midje.sweet
        elephantdb.test.common
        elephantdb.test.keyval
        [elephantdb.keyval.domain :only (to-map)]
        [elephantdb.common.config :only (read-global-config)])
  (:require [hadoop-util.core :as h]
            [hadoop-util.test :as t]
            [jackknife.core :as u]
            [jackknife.logging :as log]
            [elephantdb.common.config :as conf]
            [elephantdb.common.status :as status])
  (:import [elephantdb.persistence JavaBerkDB]))

