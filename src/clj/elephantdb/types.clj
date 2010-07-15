(ns elephantdb.types
  (:import [elephantdb Utils]))

(defn serialize-int [i]
  (Utils/serializeInt i))

(defn serialize-long [l]
  (Utils/serializeLong l))

(defn serialize-string [#^String s]
  (Utils/serializeString s))
