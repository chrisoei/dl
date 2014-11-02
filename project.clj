(defproject dl "3.1.0-SNAPSHOT"
  :description "Download manager"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [
                  [org.clojure/clojure "1.6.0"]
                  [org.clojure/data.json "0.2.5"]
                  [clj-http "1.0.0"]
                  [commons-cli/commons-cli "1.2"]
                  [commons-codec/commons-codec "1.9"]
                  [commons-io/commons-io "2.4"]
                  [org.bouncycastle/bcprov-jdk15on "1.51"]
                  [org.clojure/java.jdbc "0.3.5"]
                  [org.xerial/sqlite-jdbc "3.8.7"]
                ]
  :main ^:skip-aot dl.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
