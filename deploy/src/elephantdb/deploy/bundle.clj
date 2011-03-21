(ns elephantdb.deploy.bundle
  (:use [clojure.contrib.shell :only [sh]]
        [pallet.execute :only [local-script]]
        ))




#_ (def w (make-release!))
#_ (print (:exit w))