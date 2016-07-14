(ns twit.jobs.smoke
  (:require [onyx.job :refer [add-task register-job]]
            [onyx.tasks.core-async :as core-async-task]
            [twit.tasks.math :as math]
            [onyx.tasks.seq :as s]
            [twit.tasks.logging :refer [with-segment-logging]]
            [twit.tasks.twitter]))

(defn smoketest-job
  [batch-settings]
  (let [base-job {:workflow [[:in :out]]
                  :catalog []
                  :lifecycles []
                  :windows []
                  :triggers []
                  :flow-conditions []
                  :task-scheduler :onyx.task-scheduler/balanced}]
    (-> base-job
        (add-task (s/buffered-file-reader :in
                                          "resources/data.txt"
                                          batch-settings))
        (add-task (core-async-task/output :out
                                          (merge batch-settings
                                                 {:onyx/uniqueness-key :val}))
                  (with-segment-logging)))))

(defmethod register-job "smoke-test"
  [job-name _]
  (smoketest-job {:onyx/batch-size 1 :onyx/batch-timeout 1000}))
