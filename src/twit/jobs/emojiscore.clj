(ns twit.jobs.emojiscore
  (:require [lib-onyx.joplin :as joplin]
            [onyx.job :refer [add-task]]
            [onyx.tasks
             [core-async :as core-async-task]
             [twitter :as twitter-plugin-tasks]]
            [twit.tasks
             [reshape :as reshape]
             [twitter :as tweet]]))

(def reshape-segment
  {:text [:text]
   :id [:id]
   :created-at [:createdAt :time]
   :country [:place :countryCode]})

(defn build-job
  [twitter-config joplin-config batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job {:workflow [[:in :reshape-segment]
                             [:reshape-segment :count-emojis]
                             [:count-emojis :update-emojiscore]
                             [:update-emojiscore :out]]
                  :catalog []
                  :lifecycles []
                  :windows []
                  :triggers []
                  :flow-conditions []
                  :task-scheduler :onyx.task-scheduler/balanced}]
    (-> base-job
        (add-task (twitter-plugin-tasks/stream :in
                                               [:id :text :createdAt :place]
                                               (merge batch-settings twitter-config)))
        (add-task (reshape/reshape-segment :reshape-segment
                                           reshape-segment
                                           batch-settings))
        (add-task (tweet/count-emoji :count-emojis
                                     [:text]
                                     batch-settings))
        (add-task (tweet/update-emojiscore :update-emojiscore
                                           batch-settings)
                  (joplin/with-joplin-migrations :dev joplin-config))
        (add-task (core-async-task/output :out
                                          batch-settings)))))
