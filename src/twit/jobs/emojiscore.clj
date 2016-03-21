(ns twit.jobs.emojiscore
  (:require [onyx.job :refer [add-task]]
            [twit.tasks.math :as math]
            [twit.tasks.segment :as segment-tasks]
            [onyx.tasks.twitter :as twitter]
            [onyx.tasks.core-async :as core-async-task]))

(defn build-job
  [twitter-config batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job {:workflow [[:in :extract-tweet-info]
                             [:extract-tweet-info :out]]
                  :catalog []
                  :lifecycles []
                  :windows []
                  :triggers []
                  :flow-conditions []
                  :task-scheduler :onyx.task-scheduler/balanced}]
    (-> base-job
        (add-task (twitter/stream :in (merge batch-settings twitter-config)))
        (add-task (segment-tasks/filter-keypath :in :all [:tweet :place :country-code]))
        (add-task (segment-tasks/transform-segment-shape :extract-tweet-info {:text [:tweet :text]
                                                                              :user [:tweet :user :name]
                                                                              :country [:tweet :place :country-code]}
                                                         batch-settings))
        (add-task (core-async-task/output :out batch-settings)))))
