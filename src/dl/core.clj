(ns dl.core
  (:gen-class))

(import '(java.io File))
(import '(org.apache.commons.cli GnuParser Options))
(import '(org.apache.commons.io FileUtils))

(require '[clj-http.client :as client])
(require '[clojure.data.json :as json])
(require '[clojure.java.io :refer [file output-stream]])
(require '[clojure.java.jdbc :as jdbc])
(require '[dl.hash :as hash])

(def db {
          :subprotocol "sqlite"
          :subname (System/getenv "OEI_DL")
        })

(defn fsck
  [cmd]
  (let [shas (jdbc/query
            db
            ["SELECT sha2_256 FROM dl;"]
          )
       ]
    (doseq [x shas]
      (let [
           recs (jdbc/query
                  db
                  ["SELECT uri, sha3_256, sha1, md5, crc32, l, content FROM dl where sha2_256 = ?;"
                    (:sha2_256 x)]
                )
           row (first recs)
           content (:content row)
           h (hash/multi content)
          ]
        (println (:uri row))
        (assert (= (deref (:sha2_256 h)) (:sha2_256 x)))
        (when (not (and (= (deref (:sha3_256 h)) (:sha3_256 row))
                        (= (deref (:md5 h)) (:md5 row))
                        (= (deref (:sha1 h)) (:sha1 row))
                        (= (deref (:crc32 h)) (:crc32 row))
                        (= (deref (:l h)) (:l row))
                   ))
          (jdbc/execute!
            db
            ["UPDATE dl SET sha3_256=?, md5=?, sha1=?, crc32=?, l=? WHERE sha2_256 = ?"
               (deref (:sha3_256 h))
               (deref (:md5 h))
               (deref (:sha1 h))
               (deref (:crc32 h))
               (deref (:l h))
               (:sha2_256 x)
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
    (hash/deref-multi h)
    (jdbc/insert! db :dl
      {
        :uri (get-in r [:request :http-url])
        :referrer (get-in r [:request :headers "Referer"])
        :status (:status r)
        :content_type (get-in r [:headers "Content-Type"])
        :encoding (get-in r [:headers "Content-Encoding"])
        :sha2_256 (deref (:sha2_256 h))
        :sha3_256 (deref (:sha3_256 h))
        :sha1 (deref (:sha1 h))
        :md5 (deref (:md5 h))
        :crc32 (deref (:crc32 h))
        :content body
        :comment (.getOptionValue cmd "comment")
        :j (.getOptionValue cmd "json")
        :l (deref (:l h))
      }
    )
    (println {
      :status (:status r)
      :content_type (get-in r [:headers "Content-Type"])
      :l (deref (:l h))
      :sha2_256 (deref (:sha2_256 h))
    })
  )
)

(def ^:private query-key
  (memoize
    (fn [cmd]
      (let
        [
          query-keys '("sha2_256" "sha3_256" "sha1" "uri" "md5" "crc32" "referrer" "l")
        ]
        (first (filter #(.hasOption cmd %) query-keys))
      )
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
      (jdbc/query
        db
        [(str "SELECT content FROM dl WHERE " (query-key cmd) " = ?;")
         (.getOptionValue cmd (query-key cmd))]
      )
  )
)

(defn- display-row [x]
  (dissoc
    (assoc
      x
      :json
      (json/read-str (:j x) :key-fn keyword)
    )
    :j
  )
)

(defn examine [cmd]
  (let [rows (jdbc/query
                  db
                  [(str "SELECT sha2_256, sha3_256, sha1, md5, crc32, l, "
                        "uri, referrer, status, content_type, encoding, "
                        "compression, comment, j, created_at "
                        "FROM dl WHERE "
                        (query-key cmd) " = ?")
                   (.getOptionValue cmd (query-key cmd))])
         rs (mapv display-row rows)
       ]
    (println rs)
  )
)

(defn -main
  "This program manages downloads in a SQLITE database."
  [& args]
  (let [
       options (doto (Options.)
                  (.addOption "examine" false "Examine data")
                  (.addOption "extract" true "Extract to file")
                  (.addOption "fsck" false "Check/repair")
                  (.addOption "get" false "Get from URI")
                  (.addOption "import" true "Import from a file")
                  (.addOption "referrer" true "Referring web page")
                  (.addOption "uri" true "URI to download")
                  (.addOption "sha2_256" true "SHA-2 (256-bit) hash of file to extract")
                  (.addOption "sha3_256" true "SHA-3 (256-bit) hash of file to extract")
                  (.addOption "sha1" true "SHA-1 hash of file to extract")
                  (.addOption "md5" true "MD-5 hash of file to extract")
                  (.addOption "crc32" true "CRC32 hash of file to extract")
                  (.addOption "l" true "Length of file to extract")
                  (.addOption "comment" true "Comment")
                  (.addOption "json" true "Additional JSON")
                )
       argv (into-array args)
       parser (GnuParser.)
       cmd (.parse parser options argv)
     ]
    (assert (empty? (.getArgList cmd)))
    (if (.hasOption cmd "json")
      (json/read-str (.getOptionValue cmd "json"))
    )
    (cond
      (.hasOption cmd "examine")
        (examine cmd)
      (.hasOption cmd "extract")
        (extract cmd)
      (.hasOption cmd "fsck")
        (fsck cmd)
      (.hasOption cmd "get")
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
      (.hasOption cmd "import")
        (insert cmd
          ; simulate request
          {
            :body (FileUtils/readFileToByteArray (File. (.getOptionValue cmd "import")))
            :request {
              :http-url (.getOptionValue cmd "uri")
              :headers { "Referer" (.getOptionValue cmd "referrer") }
            }
          }
        )
      :else
        (println "No command given")
    )
  )
)
