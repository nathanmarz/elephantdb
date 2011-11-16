(ns elephantdb.types
  (:import [elephantdb Utils]))

(defprotocol ISerialize
  (serialize [x]))

(extend-protocol ISerialize
  Integer
  (serialize [x] (Utils/serializeInt x))

  Long
  (serialize [x] (Utils/serializeLong x))

  String
  (serialize [x]  (Utils/serializeString x)))

(extend (Class/forName "[B") 
  ISerialize
  {:serialize identity})
