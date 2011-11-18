(ns elephantdb.common.iface
  (:import [elephantdb Utils]))

(defprotocol IShutdownable
  (shutdown [_]))

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

(comment
  "Examples of records and protocols:"
  (defprotocol Cake
    (face [_] "Docstring for face."))

  (defrecord Icing [color]
    Cake
    (face [this] "hi."))

  "after arg vector, defrecord implement combinations of
   protocol/interface and methods."
  (defrecord ElephantClient [arg1]
    
    Shutdownable
    (shutdown [this]
      ...))

  (extend-type ElephantClient
    SomeInterface
    (wiggle [this x y] ,,,)

    OtherInterface
    (stretch [this] ,,,))

  "OR!"

  (extend ElephantClient
    SomeInterface
    {:wiggle (fn [this x y]
               ,,,)})

  "The nice thing here is that we can define a map of functions common
  to both the elephantdb client and the service and share them without
  worrying about calls between the two.")
