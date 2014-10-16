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

(defn insert [r]
  (jdbc/insert! db :dl
    {
      :uri (get-in r [:request :http-url])
      :referrer (get-in r [:request :headers "Referer"])
      :status (:status r)
      :content_type (get-in r [:headers "Content-Type"])
      :encoding (get-in r [:headers "Content-Encoding"])
      :sha2_256 (DigestUtils/sha256Hex (:body r))
      :crc32 (format "%08x" (.getValue (doto (CRC32.) (.update (:body r)))))
      :content (:body r)
      :l (count (:body r))
    })
)

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (insert (client/get "http://oei.io" {
                   :as :byte-array
                   :headers {
                     :referer "http://oei.io"
                     :user-agent "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.101 Safari/537.36"
                   }
                   :save-request? true
           })
  )
)
