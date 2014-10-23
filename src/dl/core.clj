(ns dl.core
  (:gen-class))

(import '(java.io File))
(import '(org.apache.commons.cli GnuParser Options))
(import '(org.apache.commons.io FileUtils))

(require '[clj-http.client :as client])
(require '[clojure.java.io :refer [file output-stream]])
(require '[clojure.java.jdbc :as jdbc])
(require '[dl.hash :as hash])

(def db {
          :subprotocol "sqlite"
          :subname (System/getenv "OEI_DL")
        })

(defn fsck
  [cmd]
  (let [uris (jdbc/query
            db
            ["SELECT uri FROM dl;"]
          )
       ]
    (doseq [u uris]
      (let [
           recs (jdbc/query
                  db
                  ["SELECT uri, sha2_256, sha1, md5, crc32, l, content FROM dl where uri = ?;"
                    (:uri u)]
                )
           row (first recs)
           content (:content row)
           h (hash/multi content)
          ]
        (println (:uri row))
        (assert (= (deref (:sha2_256 h)) (:sha2_256 row)))
        (when (not (and (= (deref (:md5 h))(:md5 row))
                        (= (deref (:sha1 h)) (:sha1 row))
                        (= (deref (:crc32 h)) (:crc32 row))
                        (= (deref (:l h)) (:l row))
                   ))
          (jdbc/execute!
            db
            ["UPDATE dl SET md5=?, sha1=?, crc32=?, l=? WHERE uri = ?"
               (deref (:md5 h))
               (deref (:sha1 h))
               (deref (:crc32 h))
               (deref (:l h))
               (:uri row)
            ]
          )
        )
      )
    )
  )
  (shutdown-agents)
)

(defn insert [cmd r]
  (let [body (:body r)
        h (hash/multi body)
        ]
    (jdbc/insert! db :dl
      {
        :uri (get-in r [:request :http-url])
        :referrer (get-in r [:request :headers "Referer"])
        :status (:status r)
        :content_type (get-in r [:headers "Content-Type"])
        :encoding (get-in r [:headers "Content-Encoding"])
        :sha2_256 (deref (:sha2_256 h))
        :sha1 (deref (:sha1 h))
        :md5 (deref (:md5 h))
        :crc32 (deref (:crc32 h))
        :content body
        :comment (.getOptionValue cmd "comment")
        :j (.getOptionValue cmd "json")
        :l (deref (:l h))
      }
    )
    (shutdown-agents)
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
      (.hasOption cmd "sha256")
        (jdbc/query
          db
          ["SELECT content FROM dl WHERE sha2_256 = ?" (.getOptionValue cmd "sha256")]
        )
      (.hasOption cmd "uri")
        (jdbc/query
          db
          ["SELECT content FROM dl WHERE uri = ?" (.getOptionValue cmd "uri")]
        )
      (.hasOption cmd "sha1")
        (jdbc/query
          db
          ["SELECT content FROM dl WHERE sha1 = ?" (.getOptionValue cmd "sha1")]
        )
      (.hasOption cmd "md5")
        (jdbc/query
          db
          ["SELECT content FROM dl WHERE md5 = ?" (.getOptionValue cmd "md5")]
        )
      (.hasOption cmd "crc32")
        (jdbc/query
          db
          ["SELECT content FROM dl WHERE crc32 = ?" (.getOptionValue cmd "crc32")]
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
                  (.addOption "fsck" false "Check/repair")
                  (.addOption "import" true "Import from a file")
                  (.addOption "referrer" true "Referring web page")
                  (.addOption "uri" true "URI to download")
                  (.addOption "sha256" true "SHA-2 (256-bit) hash of file to extract")
                  (.addOption "sha1" true "SHA-1 hash of file to extract")
                  (.addOption "md5" true "MD-5 hash of file to extract")
                  (.addOption "crc32" true "CRC32 hash of file to extract")
                  (.addOption "comment" true "Comment")
                  (.addOption "json" true "Additional JSON")
                )
       argv (into-array args)
       parser (GnuParser.)
       cmd (.parse parser options argv)
     ]
    (assert (empty? (.getArgList cmd)))
    (cond
      (.hasOption cmd "extract")
        (extract cmd)
      (.hasOption cmd "fsck")
        (fsck cmd)
      :else
        (insert cmd
          (if (.hasOption cmd "import")
            ; simulate request
            {
              :body (FileUtils/readFileToByteArray (File. (.getOptionValue cmd "import")))
              :request {
                :http-url (.getOptionValue cmd "uri")
                :headers { "Referer" (.getOptionValue cmd "referrer") }
              }
            }
            ; else
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
)
