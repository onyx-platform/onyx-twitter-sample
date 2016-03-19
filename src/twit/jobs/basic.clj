(ns twit.jobs.basic
  (:require [onyx.job :refer [add-task]]
            [onyx.tasks.core-async :as core-async-task]))

(defn build-job
  [batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job {:workflow [[:in :out]]
                  :catalog []
                  :lifecycles []
                  :windows []
                  :triggers []
                  :flow-conditions []
                  :task-scheduler :onyx.task-scheduler/balanced}]
    (-> base-job
        (add-task (core-async-task/input :in batch-settings))
        (add-task (core-async-task/output :out batch-settings)))))
