(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp))

(defproject elephantdb/elephantdb-thrift VERSION
  :min-lein-version "2.0.0"
  :java-source-paths ["src/jvm"]
  :javac-options ["-source" "1.6" "-target" "1.6"]
  :dependencies [[org.apache.thrift/libthrift "0.9.1"]]
  :profiles {:dev
             {:dependencies
              [[org.clojure/clojure "1.5.1"]
               [midje "1.6.3"]]
              :plugins [[lein-midje "3.1.3"]]}})
