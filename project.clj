(defproject twit "0.1.0-SNAPSHOT"
  :description "Example project for Onyx"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.onyxplatform/onyx "0.9.7-SNAPSHOT"]
                 [org.onyxplatform/lib-onyx "0.9.0.1-SNAPSHOT"]
                 [honeysql "0.6.3"]
                 [mysql/mysql-connector-java "5.1.38"]
                 [org.clojure/java.jdbc "0.4.2"]
                 [joplin.core "0.3.6"]
                 [joplin.jdbc "0.3.6"]
                 [aero "1.0.0-beta2"]
                 [org.onyxplatform/onyx-twitter "0.9.0.1-SNAPSHOT"]]
  :profiles {:dev {:jvm-opts ["-XX:-OmitStackTraceInFastThrow"]
                   :source-paths ["src"]}
             :uberjar {:aot [lib-onyx.media-driver
                             twit.start-peer
                             twit.core]
                       :uberjar-name "peer.jar"}})
