(ns twit.jobs.trending
  (:require [lib-onyx.joplin :as joplin]
            [onyx.job :refer [add-task]]
            [onyx.tasks
             [core-async :as core-async-task]
             [twitter :as twitter]]
            [twit.tasks.reshape :as reshape]
            [twit.tasks.twitter :as tweet]
            [twit.jobs :refer [register-job]]))

(def segment-pattern
  {:text [:text]
   :id [:id]
   :created-at [:createdAt :time]})

(defn trending-hashtags-job
  "Builds up the initial job skeleton. Note that while we provide
  task bundles for :reshape-segment, :split-hashtags, and :out.
  All of these task bundles deal with either pure data transformations,
  or in the case of window-trending-hashtags, a windowing aggregation.
  None of them specify a data sink or :trigger/sync target.

  This is to allow us to specify those later, depending on what kind of
  durable storage we want to use."
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

;; "Picking up where trending-hashtags-job left off, we add the root and 'leafs'
;;   to the graph for a production environment. This is input from the actual live
;;   twitter stream, a core-async output channel (since we dont care about
;;   the workflow output, just the :trigger/sync) and a :trigger/sync sending
;;   data to MySQL."

(defmethod register-job "trending-hashtags"
  [job-name {:keys [twitter-config joplin-config]}]
  (let [batch-settings {:onyx/batch-size 1
                        :onyx/batch-timeout 1000}
        connection-uri (get-in joplin-config [:environments :dev 0 :db :url])]
    (println twitter-config)
    (-> (twit.jobs.trending/trending-hashtags-job batch-settings)
        (add-task (twitter/stream :in [:id :text :createdAt] (merge batch-settings twitter-config)))
        (add-task (core-async-task/output :out (merge batch-settings {:onyx/group-by-key :hashtag
                                                                      :onyx/flux-policy :recover
                                                                      :onyx/min-peers 1
                                                                      :onyx/max-peers 1
                                                                      :onyx/uniqueness-key :id}))
                  (tweet/with-trigger-to-sql :hashtag-window connection-uri)
                  (joplin/with-joplin-migrations :dev joplin-config)))))
