(ns twit.jobs
  (:gen-class)
  (:require [aero.core :refer [read-config]]
            [clojure.java.io :as io]
            onyx.api
            twit.jobs.trending))

(defmulti build-job (fn [job-name config]
                      job-name))

(defmethod build-job "trending-hashtags"
  [job-name {:keys [twitter-config joplin-config]}]
  (let [batch-settings {:onyx/batch-size 1
                        :onyx/batch-timeout 1000}]
    (-> (twit.jobs.trending/trending-hashtags-job batch-settings)
        (twit.jobs.trending/add-prod-leafs twitter-config joplin-config batch-settings))))

(defn -main
  "Takes a job name to run, and submits it to the cluster"
  [& [job-name]]
  (let [{:keys [peer-config] :as config} (read-config (io/resource "config.edn") {:profile :docker})
        job (build-job job-name config)]
    (-> (onyx.api/submit-job peer-config job)
        (clojure.pprint/pprint))))
