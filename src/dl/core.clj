(ns dl.core
  (:gen-class))

(import '(java.util.zip CRC32))
(import '(org.apache.commons.cli GnuParser Options))
(import '(org.apache.commons.codec.digest DigestUtils))
;(import '(org.apache.commons.io IOUtils))
;(import '(org.apache.httpcomponents/httpclient HttpClients HttpGet))
;(import '(org.apache.httpcomponents/httpcore ContentType EntityUtils))
(require '[clj-http.client :as client])

(require '[clojure.java.jdbc :as jdbc])

(def db {
          :subprotocol "sqlite"
          :subname (System/getenv "OEI_DL")
        })

(defn insert [cmd r]
  (jdbc/insert! db :dl
    {
      :uri (get-in r [:request :http-url])
      :referrer (get-in r [:request :headers "Referer"])
      :status (:status r)
      :content_type (get-in r [:headers "Content-Type"])
      :encoding (get-in r [:headers "Content-Encoding"])
      :sha2_256 (DigestUtils/sha256Hex (:body r))
      :sha1 (DigestUtils/sha1Hex (:body r))
      :md5 (DigestUtils/md5Hex (:body r))
      :crc32 (format "%08x" (.getValue (doto (CRC32.) (.update (:body r)))))
      :content (:body r)
      :comment (.getOptionValue cmd "comment")
      :j (.getOptionValue cmd "json")
      :l (count (:body r))
    })
)

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (let [
       options (doto (Options.)
                  (.addOption "referrer" true "Referring web page")
                  (.addOption "uri" true "URI to download")
                  (.addOption "comment" true "Comment")
                  (.addOption "json" true "Additional JSON")
                )
       argv (into-array args)
       parser (GnuParser.)
       cmd (.parse parser options argv)
     ]
    (insert cmd
            (client/get (.getOptionValue cmd "uri") {
                     :as :byte-array
                     :headers {
                       :referer (.getOptionValue cmd "referrer")
                       :user-agent "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.101 Safari/537.36"
                     }
                     :save-request? true
             })
    )
  )
)
