(ns elephantdb.ui.middleware)

(defn wrap-configuration
  "Middleware to wrap some configuraion data."
  [handler conf-map]
  (fn [request]
    (let [request (assoc request :conf-map conf-map)]
      (handler request))))
