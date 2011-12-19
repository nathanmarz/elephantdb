(ns elephantdb.common.iface
  (:import [elephantdb Utils]))

;; Domains could also implement comparable... Anyway, the idea here is
;; that we need something that implements IDomainStore to contain
;; shards.

;; domain store, either remote or local, that should
;; be able to provide paths and connections within its guts (but have
;; no real knowledge of the filesyste, etc.


;; The IDomainStore interface is service-facing; actual elephantDB
;; applications will need a lower-level interface that reaches into
;; these domain stores to provide access to individual shard paths.
;;
;; On the other hand, we might find that it's enough to provide a path
;; to the latest version; if a "version" can be fully synched between
;; between filesystems then we're good to go.
;;
;; The filesystem implementation should not be tied to hadoop. With an
;; interface like this it becomes possible to create varying records
;; and types; if they fulfill this interface, and the interface
;; required for transfer, that should be enough.
;;
;; Protocol examples: https://gist.github.com/1495818

(defprotocol IDomainStore
  (allVersions [_]
    "Returns a sequence of available version timestamps.")
  (latestVersion [_]
    "Returns the timestamp (in seconds) of the latest versions.")
  (latestVersionPath [_]
    "Returns the timestamp (in seconds) of the latest versions.")
  (hasData [_]
    "Does the domain contain any domains at all?")
  (cleanup [_ to-keep]
    "Destroys all but the last `to-keep` versions.")
  (spec [_]
    "Returns a clojure datastructure containing the DomainStore's spec."))

(defprotocol IPreparable
  (prepare [_] "Perform preparatory steps."))

(defprotocol IShutdownable
  (shutdown [_]))


