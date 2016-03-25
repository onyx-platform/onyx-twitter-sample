(defproject twit "0.1.0-SNAPSHOT"
  :description "Example project for Onyx"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.onyxplatform/onyx "0.9.0-beta2"]
                 [honeysql "0.6.3"]
                 [mysql/mysql-connector-java "5.1.38"]
                 [org.clojure/java.jdbc "0.4.2"]
                 [joplin.core "0.3.6"]
                 [joplin.jdbc "0.3.6"]
                 [aero "0.3.0"]
                 [org.onyxplatform/onyx-twitter "0.9.0.0-SNAPSHOT"]]
  :profiles {:dev {:jvm-opts ["-XX:-OmitStackTraceInFastThrow"]
                   :dependencies [[org.clojure/tools.namespace "0.2.11"]]
                   :source-paths ["src"]}})
