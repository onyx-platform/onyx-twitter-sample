(ns twit.jobs.emojiscore
  (:require
   [twit.tasks.math :as math]
   [onyx.schema :as os]
   [schema.core :as s]
   [onyx.job :refer [add-task]]
   [twit.tasks.twitter :as tweet]
   [twit.tasks.segment :as segment-tasks]
   [onyx.tasks.twitter :as twitter-plugin-tasks]
   [lib-onyx.joplin :as joplin]
   [onyx.tasks.core-async :as core-async-task]))

(defn build-job
  [twitter-config joplin-config batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job {:workflow [[:in :extract-tweet-info]
                             [:extract-tweet-info :count-emojis]
                             [:count-emojis :bucket-emojis]
                             [:bucket-emojis :out]]
                  :catalog []
                  :lifecycles []
                  :windows []
                  :triggers []
                  :flow-conditions []
                  :task-scheduler :onyx.task-scheduler/balanced}]
    (-> base-job
        (add-task (twitter-plugin-tasks/stream :in (merge batch-settings twitter-config)))
        (add-task (segment-tasks/filter-keypath :in :all [:tweet :place :country-code]))
        (add-task (segment-tasks/transform-segment-shape :extract-tweet-info {:text [:tweet :text]
                                                                              :user [:tweet :user :name]
                                                                              :created-at [:tweet :created-at]
                                                                              :country [:tweet :place :country-code]
                                                                              :id [:tweet :id]} batch-settings))
        (add-task (tweet/add-emoji-count :count-emojis [:text] [:emoji-count] batch-settings))
        (add-task (tweet/window-emojiscore-by-country :bucket-emojis batch-settings)
                  (joplin/with-joplin-migrations :dev joplin-config))
        (add-task (core-async-task/output :out batch-settings)))))
