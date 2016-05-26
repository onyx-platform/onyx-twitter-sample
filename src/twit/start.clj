(ns twit.start
  (:require [aero.core :refer [read-config]]
            [lib-onyx.peer :as peer]
            [lib-onyx.media-driver]
            [taoensso.timbre.appenders.core :refer [println-appender]])
  (:gen-class))

(defn -main [& args]
  (let [{:keys [env-config peer-config]}
        (read-config (clojure.java.io/resource "config.edn"))]
    (peer/start-peer 5 peer-config env-config)))
