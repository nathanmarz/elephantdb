(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp))

(defproject elephantdb/elephantdb-bdb VERSION
  :min-lein-version "2.0.0"
  :java-source-paths ["src/jvm"]
  :javac-options ["-source" "1.6" "-target" "1.6"]
  :repositories {"oracle" "http://download.oracle.com/maven"}
  :dependencies [[elephantdb/elephantdb-core ~VERSION]
                 [com.sleepycat/je "5.0.58"]]
  :profiles {:dev
             {:dependencies
              [[midje "1.5.1"]]
              :plugins [[lein-midje "3.0.1"]]}})
