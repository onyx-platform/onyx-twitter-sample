(ns twit.core
  (:gen-class)
  (:require [onyx.api]
            [aero.core :refer [read-config]]
            [clojure.java.io :as io]
            [twit.jobs :refer [register-job]]
            [twit.jobs.trending]
            [twit.jobs.basic]))

(defn -main
  "Takes a job name to run, and submits it to the cluster"
  [& [job-name]]
  (let [{:keys [peer-config] :as config} (read-config (io/resource "config.edn") {:profile :docker})
        job (register-job job-name config)]
    (-> (onyx.api/submit-job peer-config job)
        (clojure.pprint/pprint))))
