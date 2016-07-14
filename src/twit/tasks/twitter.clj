(ns twit.tasks.twitter
  (:require [schema.core :as s]
            [taoensso.timbre :refer [info]]))

(defn split-on-hashtags
  "Splits segments into multiple segments containing an original hashtag,
  the literal hash of the original tweet id and the hashtag, and the original
  tweet-id"
  [id-ks text-ks segment]
  (try
    (let [matcher (partial re-seq #"\S*#(?:\[[^\]]+\]|\S+)")
          created-at (get segment :created-at (java.util.Date.))
          id (get-in segment id-ks)]
      (mapv (fn [hashtag]
              (assoc {:id (hash (str hashtag id))
                      :tweet-id id
                      :created-at created-at} :hashtag hashtag)) (matcher (get-in segment text-ks))))
    (catch Exception e
      (do (info "Could not split on hashtags: " segment)
          {}))))

(defn emit-hashtag-ids
  "Splits a tweet into individual hashtags with a common id, and a tweet id"
  [task-name id-ks text-ks task-opts]
  {:task {:task-map (merge {:onyx/name task-name
                            :onyx/type :function
                            :onyx/fn ::split-on-hashtags
                            ::arg1 id-ks
                            ::arg2 text-ks
                            :onyx/params [::arg1 ::arg2]} task-opts)}
   :schema {:task-map {::arg1 [s/Any]
                       ::arg2 [s/Any]}}})

(s/defn window-trending-hashtags
  [task-name window-id]
  {:task {:windows [{:window/id window-id
                     :window/task task-name
                     :window/type :global
                     :window/window-key :created-at
                     :window/aggregation :onyx.windowing.aggregation/count}]}})

(s/defn with-trigger-to-sql
  [window-id :- s/Keyword connection-uri :- s/Str]
  (fn [task-definition]
    (-> task-definition
        (update-in [:task :triggers] conj
                   {:trigger/window-id window-id
                    :trigger/refinement :onyx.refinements/accumulating
                    :trigger/on :onyx.triggers/segment
                    :trigger/threshold [5 :elements]
                    :trigger/sync :twit.persist.sql/upsert-trending
                    :sql/connection-uri {:connection-uri connection-uri}})
        (update-in [:schema :triggers] conj
                   {:sql/connection-uri {:connection-uri s/Str}}))))

(defn with-trigger-to-atom
  [window-id atom-id]
  (fn [task-definition]
    (let [task-name (get-in task-definition [:task :task-map :onyx/name])]
      (-> task-definition
          (update-in [:task :triggers] conj
                     {:trigger/window-id window-id
                      :trigger/refinement :onyx.refinements/accumulating
                      :trigger/on :onyx.triggers/segment
                      :trigger/threshold [5 :elements]
                      :lib-onyx.persist.atom/atom-id atom-id
                      :trigger/sync :lib-onyx.persist.atom/persist-trending})
          (update-in [:task :lifecycles] conj
                     {:lifecycle/task task-name
                      :lib-onyx.persist.atom/atom-id atom-id
                      :lifecycle/calls :lib-onyx.persist.atom/calls})))))
