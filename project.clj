(defproject twit "0.1.0-SNAPSHOT"
  :description ""
  :url ""
  :license {:name ""
            :url ""}
  :dependencies [[aero "1.0.0-beta2"]
                 [joplin.core "0.3.6"]
                 [joplin.jdbc "0.3.6"]
                 [mysql/mysql-connector-java "5.1.38"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/java.jdbc "0.4.2"]
                 [org.clojure/tools.cli "0.3.5"]
                 [org.onyxplatform/lib-onyx "0.9.0.1"]
                 [org.onyxplatform/onyx "0.9.7-SNAPSHOT"]
                 [org.onyxplatform/onyx-twitter "0.9.0.1"]]
  :profiles {:dev {:jvm-opts ["-XX:-OmitStackTraceInFastThrow"]
                   :source-paths ["src"]
                   :global-vars {*assert* false}}
             :uberjar {:aot [lib-onyx.media-driver
                             twit.core]
                       :global-vars {*assert* false}
                       :uberjar-name "peer.jar"}})
