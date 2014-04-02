(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp))

(defproject elephantdb/elephantdb-leveldb VERSION
  :min-lein-version "2.0.0"
  :java-source-paths ["src/jvm"]
  :javac-options ["-source" "1.6" "-target" "1.6"]
  :repositories {"fusesource.nexus.snapshot" "http://repo.fusesource.com/nexus/content/groups/public-snapshots"}
  :dependencies [[elephantdb/elephantdb-core ~VERSION]
                 [org.fusesource.leveldbjni/leveldbjni-all "1.7"]]
  :profiles {:dev
             {:dependencies
              [[midje "1.6.3"]]
              :plugins [[lein-midje "3.1.3"]]}})
