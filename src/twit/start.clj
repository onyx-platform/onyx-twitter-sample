(ns twit.start
  (:require [aero.core :refer [read-config]]
            [lib-onyx.peer :as peer]
            [lib-onyx.media-driver])
  (:gen-class))

(defn -main [& args]
  (let [{:keys [env-config peer-config] :as config}
        (read-config (clojure.java.io/resource "config.edn") {:profile :docker})]
    (clojure.pprint/pprint config)
    (peer/start-peer 5 peer-config env-config)))
