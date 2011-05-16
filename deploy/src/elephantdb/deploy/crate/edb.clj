(ns elephantdb.deploy.crate.edb
  (:import org.antlr.stringtemplate.StringTemplate)
  (:use 
   [elephantdb.deploy.crate
    [edb-configs :only [remote-file-local-conf! upload-global-conf!]]
    [leiningen :only [leiningen]]]
   [pallet thread-expr]
   [pallet.resource
    [package :only [package]]
    [exec-script :only [exec-script]]
    [remote-file :only [remote-file]]
    [directory :only [directory, directories]]]
   [pallet.execute :only [local-script]]
   [pallet.crate
    [java :only [java]]]))

(def local-template-dir "templates/")

(def service-dir "/service/elephantdb/")
(def s3-configs-dir "/configs/elephantdb/")
(def service-subdirs (map (partial str service-dir)
                          ["releases" "shared" "log"]))

(defn- template-context [req]
  {"GLOBALCONF" (format "%s/%s/global-conf.clj" s3-configs-dir (:ring req))
   "TOKEN" (int (/ (System/currentTimeMillis) 1000))})

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

(defn render-remote-file! [req rel-path]
  (let [dst-path (str service-dir rel-path)
        src-path (str local-template-dir rel-path)
        render (render-template! src-path (template-context req))]
    (-> req
        (remote-file dst-path :content render :mode 744))))

(defn filelimits [req]
  (-> req
    (remote-file
      "/etc/security/limits.conf"
      :content "root\tsoft\tnofile\t500000\nroot\thard\tnofile\t500000\nelephantdb\tsoft\tnofile\t500000\nelephantdb\thard\tnofile\t500000\n"
      :no-versioning true
      :overwrite-changes true)))

(defn setup [req]
  (-> req
      (java :sun :jdk)
      (leiningen)
      (directories service-subdirs :action :create)
      (render-remote-file! "run")
      (render-remote-file! "log/run")))

(defn deploy [req & {:keys [new-token] :or {new-token true}}]
  (let [time (System/currentTimeMillis)
        releases-dir (str service-dir "/releases/")

        new-release-dir (str releases-dir time)
        current-sym-link (str releases-dir "current")
        new-release-file (str new-release-dir "/release.tar.gz")
        local-conf-file (str new-release-dir "/local-conf.clj")]
    (make-release!)
    (-> req
        (directory new-release-dir :action :create)
        (remote-file new-release-file :local-file "release.tar.gz")
        (if-> new-token
              (render-remote-file! "shared/run-elephant.sh"))
        (exec-script
         (cd ~new-release-dir)
         (tar xvzfop ~new-release-file)
         (export "LEIN_ROOT=1")
         (lein deps)
         (rm -f "run-elephant.sh")
         (ln -sf "../../shared/run-elephant.sh" "."))
        (remote-file-local-conf! local-conf-file)
        (exec-script
         (rm -f ~current-sym-link)
         (ln -sf ~new-release-dir ~current-sym-link))
        (upload-global-conf!))))


