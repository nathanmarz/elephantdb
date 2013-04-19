# ElephantDB Client

A Clojure client interface to ElephantDB.

## Example Usage

Assuming a domain with string keys and values serialized to byte
arrays:

```clojure
(use 'elephantdb.client)

;; serialize strings to byte arrays
(def keys (map #(.getBytes %) ["biggie" "tupac"]))

(with-elephant "127.0.0.1" 3578 connection
   (let [results-map (multi-get connection "rappers" keys)]
       (into {} (for [[k v] results-map]
         [(String. k) (String. v)]))))

;; => {"biggie" "east-coast", "tupac" "west-coast"}
```
