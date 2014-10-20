(ns dl.core
  (:gen-class))

(import '(java.util.zip CRC32))
(import '(org.apache.commons.cli GnuParser Options))
(import '(org.apache.commons.codec.digest DigestUtils))
(require '[clj-http.client :as client])
(require '[clojure.java.io :refer [file output-stream]])
(require '[clojure.java.jdbc :as jdbc])

(def db {
          :subprotocol "sqlite"
          :subname (System/getenv "OEI_DL")
        })

(defn insert [cmd r]
  (let [body (:body r)]
    (jdbc/insert! db :dl
      {
        :uri (get-in r [:request :http-url])
        :referrer (get-in r [:request :headers "Referer"])
        :status (:status r)
        :content_type (get-in r [:headers "Content-Type"])
        :encoding (get-in r [:headers "Content-Encoding"])
        :sha2_256 (DigestUtils/sha256Hex body)
        :sha1 (DigestUtils/sha1Hex body)
        :md5 (DigestUtils/md5Hex body)
        :crc32 (format "%08x" (.getValue (doto (CRC32.) (.update body))))
        :content body
        :comment (.getOptionValue cmd "comment")
        :j (.getOptionValue cmd "json")
        :l (count body)
      }
    )
  )
)

(defn- write-result [cmd result]
  (with-open [out (output-stream (file (.getOptionValue cmd "extract")))]
    (.write out (:content (first result)))
  )
)

(defn extract [cmd]
  (write-result
    cmd
    (cond
      (.hasOption cmd "uri")
        (jdbc/query
          db
          ["SELECT content FROM dl WHERE uri = ?" (.getOptionValue cmd "uri")]
        )
      (.hasOption cmd "referrer")
        (jdbc/query
          db
          ["SELECT content FROM dl WHERE referrer = ?" (.getOptionValue cmd "referrer")]
        )
    )
  )
)

(defn -main
  "This program manages downloads in a SQLITE database."
  [& args]
  (let [
       options (doto (Options.)
                  (.addOption "extract" true "Extract to file")
                  (.addOption "referrer" true "Referring web page")
                  (.addOption "uri" true "URI to download")
                  (.addOption "comment" true "Comment")
                  (.addOption "json" true "Additional JSON")
                )
       argv (into-array args)
       parser (GnuParser.)
       cmd (.parse parser options argv)
     ]
    (assert (empty? (.getArgList cmd)))
    (if (.hasOption cmd "extract")
      (extract cmd)
    ;else
      (insert cmd
              (client/get (.getOptionValue cmd "uri") {
                       :as :byte-array
                       :headers {
                         :referer (.getOptionValue cmd "referrer")
                         :user-agent (System/getenv "OEI_USER_AGENT")
                       }
                       :save-request? true
               })
      )
    )
  )
)
