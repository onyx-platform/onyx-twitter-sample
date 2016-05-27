(ns twit.start-peer
  (:require [aero.core :refer [read-config]]
            [lib-onyx.peer :as peer]
            [lib-onyx.media-driver]

            ;; Load plugin classes on peer start
            [lib-onyx.joplin]
            [joplin.jdbc.database]
            [onyx.plugin.core-async]
            [onyx.plugin.twitter]
            ;; Load persisting plugins
            [twit.persist.sql]
            ;; Load our tasks
            [twit.tasks.math]
            [twit.tasks.reshape]
            [twit.tasks.twitter]
            )
  (:gen-class))

(defn -main [& args]
  (println args)
  (assert (= 1 (count args)) "Number of peers not specified in start-peer/-main")
  (let [{:keys [env-config peer-config] :as config}
        (read-config (clojure.java.io/resource "config.edn") {:profile :docker})
        n-peers (Integer/parseInt (first args))]
    (clojure.pprint/pprint config)
    (peer/start-peer n-peers peer-config env-config)))
