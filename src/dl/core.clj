(ns dl.core
  (:gen-class))

(import '(org.apache.commons.cli GnuParser Options))
(require '[clojure.java.jdbc :as jdbc])

(def db {
          :subprotocol "sqlite"
          :subname (System/getenv "OEI_DL")
        })

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!")
  (println (jdbc/query db ["SELECT COUNT(*) from dl;"]))
)
