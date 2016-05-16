(ns twit.jobs.trending
  (:require [lib-onyx.joplin :as joplin]
            [onyx.job :refer [add-task]]
            [onyx.tasks
             [core-async :as core-async-task]
             [twitter :as twitter]]
            [twit.tasks
             [reshape :as reshape]
             [twitter :as tweet]]))

(def reshape-segment
  {:text [:text]
   :id [:id]
   :created-at [:createdAt :time]})

(defn build-job
  [twitter-config joplin-config batch-settings]
  (let [base-job {:workflow [[:in :reshape-segment]
                             [:reshape-segment :split-hashtags]
                             [:split-hashtags :out]]
                  :catalog []
                  :lifecycles []
                  :windows []
                  :triggers []
                  :flow-conditions []
                  :task-scheduler :onyx.task-scheduler/balanced}]
    (-> base-job
        (add-task
         (twitter/stream :in [:id :text :createdAt]
                         (merge batch-settings twitter-config)))
        (add-task
         (reshape/reshape-segment :reshape-segment reshape-segment batch-settings))
        (add-task
         (tweet/emit-hashtag-ids :split-hashtags [:id] [:text] batch-settings))
        (add-task
         (core-async-task/output :out batch-settings)
         (tweet/with-counting-hashtags :trending-hashtags)
         (tweet/with-syncing-to-sql :trending-hashtags)
         (joplin/with-joplin-migrations :dev joplin-config)))))
