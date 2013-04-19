# ElephantDB Client

A Clojure client interface to ElephantDB.

## Example Usage

Assuming a domain with string keys and values serialized to byte
arrays:

```clojure
(use 'elephantdb.client)

;; serialize strings to byte arrays
(defn serialize-strings [coll]
  (map #(.getBytes %) coll))

;; deserialize results map back to strings
(defn deserialize-strings [m]
  (into {} (for [[k v] m]
             [(String. k) (String. v)])))

;; deserialize the key/value pairs back to Strings
(with-elephant "127.0.0.1" 3578 connection
  (deserialize-strings (multi-get connection "rappers" (serialize-strings ["biggie" "tupac"])))

;; => {"biggie" "east-coast", "tupac" "west-coast"}
```
