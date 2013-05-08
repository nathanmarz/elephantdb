(ns elephantdb.ui.handler
  (:use compojure.core
        ring.adapter.jetty
        [hiccup core page def element]
        [hiccup.bootstrap middleware page element]
        [elephantdb.ui thrift middleware])
  (:require [compojure.handler :as handler]
            [compojure.route :as route]
            [elephantdb.client :as c]))

(def VERSION "0.4.4-SNAPSHOT")

;; ## Thrift client functions

(defn node-status [host port]
  (c/with-elephant host port c
    (cond (c/updating? c) [:span {:class "label label-info"} "Loading"]
          (c/fully-loaded? c) [:span {:class "label label-success"} "Ready"]
          :else [:span {:class "label label-error"} "Error"])))

(defn domain-status [host port]
  (c/with-elephant host port c
    (when-let [statuses (c/get-status c)]
      (for [[domain status] (.get_domain_statuses statuses)]
        [(link-to (str "/node/" host "/domain/" domain) domain) (domain-status->elem status)]))))

(defn domain-metadata [host port domain]
  (c/with-elephant host port c
    (when-let [metadata (c/get-domain-metadata c domain)]
      ;(expand-domain-metadata metadata)
      [:dl.dl-horizontal
       [:dt "Latest Remote Version"] [:dd (.get_remote_version metadata)]
       [:dt "Latest Local Version"] [:dd (.get_local_version metadata)]
       [:dt "Shard Set"] [:dd (.get_shard_set metadata)]
       [:dt "Shard Count"] [:dd (-> (.get_domain_spec metadata) (.get_num_shards))]
       [:dt "Coordinator"] [:dd  [:code (-> (.get_domain_spec metadata) (.get_coordinator))]]
       [:dt "Shard Scheme"] [:dd [:code (-> (.get_domain_spec metadata) (.get_shard_scheme))]]])))

;; ## Views

(defn template [title & body]
  (html5
   [:head
    [:meta {:charset "utf-8"}]
    [:meta {:name "viewport" :content "width=device-width initial-scale=1.0"}]
    [:title title]
    (include-bootstrap)]
   [:body
    [:div.page-header
     [:h1 "ElephantDB " [:small VERSION]]
     ]
    [:div.container {:id "content"}
     body]]))

(defn index [conf-map]
  (template "ElephantDB"
            [:ul.breadcrumb
             [:li.active "Cluster"]]
            [:div
             (table
              :styles [:condensed]
              :head ["Hostname" "Status"]
              :body (for [h (:hosts conf-map)]
                      [(link-to (str "/node/" h) h) (node-status h (:port conf-map))]))
             ]
            [:div
             [:p "Configuration"]
             [:pre
              (let [sw (java.io.StringWriter.)]
                (clojure.pprint/pprint conf-map sw)
                (str sw))]
             ]))

(defn node [conf-map id]
  (template (str "ElephantDB | " id)
            [:ul.breadcrumb
             [:li (link-to "/" "Cluster") [:span.divider "/"]]
             [:li.active id]]
            [:div
             (table
              :styles [:condensed]
              :head ["Domain" "Status"]
              :body (domain-status id (:port conf-map))
              )]))

(defn domain [conf-map id domain]
  (template (str "ElephantDB | " id " | " domain)
            [:ul.breadcrumb
             [:li (link-to "/" "Cluster") [:span.divider "/"]]
             [:li (link-to (str "/node/" id) id) [:span.divider "/"]]
             [:li.active domain]]
            [:h2 (str domain "@" id)]
            [:div
             (domain-metadata id (:port conf-map) domain)]))

;; ## Routes

(defroutes app-routes
  (GET "/"  {conf-map :conf-map} (index conf-map))
  (GET "/node/:id" {conf-map :conf-map {id :id} :params} (node conf-map id))
  (GET "/node/:id/domain/:d" {conf-map :conf-map {id :id d :d} :params} (domain conf-map id d))
  (route/resources "/")
  (route/not-found "Not Found"))

(def app
  (wrap-bootstrap-resources (handler/site app-routes)))

(defn start-server! [conf-map]
  (let [port (:ui-port conf-map)]
    (run-jetty (wrap-configuration app conf-map) {:port port})))
