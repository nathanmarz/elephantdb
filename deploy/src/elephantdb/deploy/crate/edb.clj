(ns elephantdb.deploy.crate.edb
  (:use pallet.thread-expr
        [clojure.string :only (join)]
        [elephantdb.deploy.crate.edb-configs :only (remote-file-local-conf! upload-global-conf!)]
        [elephantdb.deploy.crate.leiningen :only (leiningen)]
        [pallet.resource.package :only (package)]
        [pallet.resource.exec-script :only (exec-script)]
        [pallet.resource.remote-file :only (remote-file)]
        [pallet.resource.directory :only (directory directories)]
        [pallet.execute :only (local-script)]
        [pallet.phase :only (phase-fn)]
        [pallet.crate.java :only (java)])
  (:import org.antlr.stringtemplate.StringTemplate))

;; TODO: Fix the way we create these dirs. Normalize on trailing
;; slashes, etc.
;;
;; TODO: Pull s3-configs-dir out of the pallet map.
(def local-template-dir "templates/")
(def service-dir "/service/elephantdb/")
(def s3-configs-dir "/configs/elephantdb/")
(def service-subdirs (map (partial str service-dir)
                          ["releases" "shared" "log"]))

(defn- template-context [session]
  {"GLOBALCONF" (str s3-configs-dir
                     (-> session :environment :ring)
                     "/global-conf.clj")})

(defn make-release! []
  (let [filename "../release.tar.gz"]
    (local-script
     (cd "dist/")
     (rm -f ~filename)
     (tar cvzf ~filename "."))))

(defn- render-template! [template-path context]
  (let [template (StringTemplate. (slurp template-path))]
    (doseq [[k v] context]
      (.setAttribute template k v))
    (str template)))

(defn render-remote-file! [session rel-path]
  (let [dst-path (str service-dir rel-path)
        src-path (str local-template-dir rel-path)
        render (render-template! src-path (template-context session))]
    (-> session
        (remote-file dst-path :content render :mode 744))))

(defn filelimits [session fd-limit user-seq]
  (-> session
      (remote-file
       "/etc/security/limits.conf"
       :content
       (->> user-seq
            (mapcat (fn [user]
                      [(format "%s\tsoft\tnofile\t%s" user fd-limit)
                       (format "%s\thard\tnofile\t%s" user fd-limit)]))
            (join "\n"))
       :no-versioning true
       :overwrite-changes true)))

(def setup
  (phase-fn
   (java :sun :jdk)
   (leiningen)
   (directories service-subdirs :action :create)
   (render-remote-file! "run")
   (render-remote-file! "log/run")))

(defn deploy [session]
  (let [time (System/currentTimeMillis)
        releases-dir (str service-dir "releases/")
        new-release-dir (str releases-dir time)
        current-sym-link (str releases-dir "current")
        new-release-file (str new-release-dir "/release.tar.gz")
        local-conf-file (str new-release-dir "/local-conf.clj")]
    (make-release!)
    (-> session
        (directory new-release-dir :action :create)
        (remote-file new-release-file :local-file "release.tar.gz")
        (render-remote-file! "shared/run-elephant.sh")
        (exec-script (cd ~new-release-dir)
                     (tar xvzfop ~new-release-file)
                     (export "LEIN_ROOT=1")
                     (lein deps)
                     (rm -f "run-elephant.sh")
                     (ln -sf "../../shared/run-elephant.sh" "."))
        (remote-file-local-conf! local-conf-file)
        (exec-script (rm -f ~current-sym-link)
                     (ln -sf ~new-release-dir ~current-sym-link))
        (upload-global-conf!))))
