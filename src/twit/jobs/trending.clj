(ns twit.jobs.trending
  (:require [lib-onyx.joplin :as joplin]
            [onyx.job :refer [add-task]]
            [onyx.tasks
             [core-async :as core-async-task]
             [twitter :as twitter]]
            [twit.tasks
             [reshape :as reshape]
             [twitter :as tweet]]))

(def segment-pattern
  {:text [:text]
   :id [:id]
   :created-at [:createdAt :time]})

(defn trending-hashtags-job
  "Emit the transform parts of the job, steps that do not change between
  test or production modes."
  [batch-settings]
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
        (add-task (reshape/reshape-segment :reshape-segment segment-pattern batch-settings))
        (add-task (tweet/emit-hashtag-ids :split-hashtags [:id] [:text] batch-settings))
        (add-task (tweet/window-trending-hashtags :out :hashtag-window)))))

(defn add-test-leafs
  [job batch-settings]
  (let [aggregation-settings
        {:onyx/group-by-key :hashtag
         :onyx/flux-policy :recover
         :onyx/min-peers 1
         :onyx/max-peers 1
         :onyx/uniqueness-key :id}]
    (-> job
        (add-task (core-async-task/input :in batch-settings))
        (add-task (core-async-task/output :out (merge batch-settings aggregation-settings))
                  (tweet/with-trigger-to-atom :hashtag-window :test-atom)))))

(defn add-prod-leafs
  "Adds leaf and root nodes for a production workflow. In this case,
  SQL output and twitter stream input."
  [job twitter-config joplin-config batch-settings]
  (let [connection-uri (get-in joplin-config [:environments :dev 0 :db :url])]
    (-> job
        (add-task (twitter/stream :in [:id :text :createdAt] (merge batch-settings twitter-config)))
        (add-task (core-async-task/output :out (merge batch-settings {:onyx/group-by-key :hashtag
                                                                      :onyx/flux-policy :recover
                                                                      :onyx/min-peers 1
                                                                      :onyx/max-peers 1
                                                                      :onyx/uniqueness-key :id}))
                  (tweet/with-trigger-to-sql :hashtag-window connection-uri)
                  (joplin/with-joplin-migrations :dev joplin-config)))))
