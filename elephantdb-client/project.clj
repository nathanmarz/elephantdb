(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp))

(defproject elephantdb/elephantdb-client VERSION
  :description "A client interface to ElephantDB"
  :java-source-paths ["src/jvm"]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [ch.qos.logback/logback-classic "1.0.11"]
                 [org.slf4j/jul-to-slf4j "1.7.4"]
                 [org.slf4j/jcl-over-slf4j "1.7.4"]
                 [org.slf4j/log4j-over-slf4j "1.7.4"]
                 [elephantdb/elephantdb-thrift ~VERSION
                  :exclusions [org.slf4j/slf4j-api]]]
  :profiles {:dev
             {:dependencies [[midje "1.5.1"]]
              :plugins [[lein-midje "3.0.1"]]}}
  :source-paths ["src/clj"])
