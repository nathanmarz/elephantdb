(ns elephantdb.common.interface)

(defprotocol Shutdownable
  (shutdown [_]))

(comment
  "Example:"
  (defprotocol )

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
