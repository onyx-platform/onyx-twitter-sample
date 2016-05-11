(ns twit.jobs.trending
  (:require [lib-onyx.joplin :as joplin]
            [onyx
             [job :refer [add-task]]
             [schema :as os]]
            [onyx.tasks
             [core-async :as core-async-task]
             [twitter :as twitter-plugin-tasks]]
            [schema.core :as s]
            [twit.tasks
             [math :as math]
             [reshape :as reshape]
             [twitter :as tweet]]))

(defn build-job
  [twitter-config joplin-config batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job {:workflow [[:in :extract-tweet-info]
                             [:extract-tweet-info :split-hashtags]
                             [:split-hashtags :trending-view]
                             [:trending-view :out]]
                  :catalog []
                  :lifecycles []
                  :windows []
                  :triggers []
                  :flow-conditions []
                  :task-scheduler :onyx.task-scheduler/balanced}]
    (-> base-job
        (add-task (twitter-plugin-tasks/stream :in (merge batch-settings twitter-config)))
        (add-task (reshape/transform-segment-shape
                   :extract-tweet-info {:text [:tweet :text]
                                        :id [:tweet :id]
                                        :created-at [:tweet :created-at]} batch-settings))
        (add-task (tweet/emit-hashtag-ids :split-hashtags [:id] [:text] batch-settings))
        (add-task (tweet/window-trending-hashtags :trending-view batch-settings)
                  (joplin/with-joplin-migrations :dev joplin-config))
        (add-task (core-async-task/output :out batch-settings)))))
