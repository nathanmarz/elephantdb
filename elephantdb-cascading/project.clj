(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp))

(defproject elephantdb/elephantdb-cascading VERSION
  :min-lein-version "2.0.0"
  :java-source-paths ["src/jvm"]
  :javac-options ["-source" "1.6" "-target" "1.6"]
  :jvm-opts ["-server" "-Xmx768m"]
  :repositories {"conjars" "http://conjars.org/repo"}
  :dependencies [[elephantdb/elephantdb-core ~VERSION]
                 [cascading/cascading-hadoop "2.0.8"
                  :exclusions [org.codehaus.janino/janino
                               org.apache.hadoop/hadoop-core]]]
  :profiles {:provided
             {:dependencies [[org.apache.hadoop/hadoop-core "0.20.2"]]}
             :dev
             {:dependencies
              [[elephantdb/elephantdb-bdb ~VERSION]
               [hadoop-util "0.3.0"]
               [jackknife "0.1.2"]
               [midje "1.5.1"]]
              :plugins [[lein-midje "3.0.1"]]}
             :cascading-2.1
             {:dependencies [[cascading/cascading-hadoop "2.1.6"]]}})
