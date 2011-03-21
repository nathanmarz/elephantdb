(ns elephantdb.deploy.leiningen
  (:require
   [pallet.resource.remote-file :as remote-file]
   [pallet.resource.exec-script :as exec-script]))

(def download-url "https://github.com/technomancy/leiningen/raw/stable/bin/lein")

(defn leiningen [request]
  (-> request
      (remote-file/remote-file
       "/usr/local/bin/lein"
       :url download-url
       :owner "root"
       :mode 755)
      (exec-script/exec-script
       (export "LEIN_ROOT=1")
       ("/usr/local/bin/lein"))))
