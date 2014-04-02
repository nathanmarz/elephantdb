(def VERSION (slurp "VERSION"))
(def MODULES (-> "MODULES" slurp (.split "\n")))
(def DEPENDENCIES (for [m MODULES] [(symbol (str "elephantdb/" m)) VERSION]))

(eval `(defproject elephantdb/elephantdb ~VERSION
         :description "Distributed database specialized in exporting key/value data from Hadoop"
         :url "https://github.com/nathanmarz/elephantdb"
         :license {:name "Eclipse Public License"
                   :url "http://www.eclipse.org/legal/epl-v10.html"}
         :mailing-list {:name "ElephantDB user mailing list"
                        :archive "https://groups.google.com/d/forum/elephantdb-user"
                        :post "elephantdb-user@googlegroups.com"}
         :min-lein-version "2.0.0"
         :dependencies [~@DEPENDENCIES]
         :plugins [[~'lein-sub "0.3.0"]]
         :sub [~@MODULES]
         :profiles {:dev {:dependencies [[~'midje "1.6.3"]]
                          :plugins [[~'lein-midje "3.1.3"]]}}))
