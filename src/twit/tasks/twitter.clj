(ns twit.tasks.twitter
  (:require [schema.core :as s]))

(defn count-emojis
  [text-ks segment]
  (let [emoji-count
        (reduce (fn [acc char]
                  (if (<= (int Character/MIN_SURROGATE) (int char) (int Character/MAX_SURROGATE))
                    (inc acc)
                    acc)) 0 (get-in segment text-ks))]
    (assoc-in segment [:emoji-count] emoji-count)))

(defn count-emoji
  "Counts the emojis located at the emoji-string path and assoc's the counts to the result-path"
  [task-name text-ks task-opts]
  {:task {:task-map (merge {:onyx/name task-name
                            :onyx/type :function
                            :onyx/fn ::count-emojis
                            ::text-ks text-ks
                            :onyx/params [::text-ks]}
                           task-opts)}
   :schema {:task-map {::text-ks [s/Any]}}})

(defn split-on-hashtags
  "Splits segments into multiple segments containing an original hashtag,
  the literal hash of the original tweet id and the hashtag, and the original
  tweet-id"
  [id-ks text-ks segment]
  (let [matcher (partial re-seq #"\S*#(?:\[[^\]]+\]|\S+)")
        created-at (get segment :created-at (java.util.Date.))
        id (get-in segment id-ks)]
    (mapv (fn [hashtag]
            (assoc {:id (hash (str hashtag id))
                    :tweet-id id
                    :created-at created-at} :hashtag hashtag)) (matcher (get-in segment text-ks)))))

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

(defn update-emojiscore
  ([task-name task-opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/type :function
                             :onyx/group-by-key :country
                             :onyx/flux-policy :continue
                             :onyx/uniqueness-key :id
                             :onyx/min-peers 1
                             :onyx/max-peers 1
                             :onyx/fn :clojure.core/identity}
                            task-opts)
           :windows [{:window/id (keyword (str task-name "-" "window"))
                      :window/task task-name
                      :window/type :global
                      :window/window-key :created-at
                      :window/aggregation [:onyx.windowing.aggregation/average :emoji-count]}]
           :triggers [{:trigger/window-id (keyword (str task-name "-" "window"))
                       :trigger/refinement :onyx.refinements/accumulating
                       :trigger/on :onyx.triggers/segment
                       :trigger/threshold [5 :elements]
                       :trigger/sync :twit.persist.sql/upsert-emojicount}]}}))

(defn window-trending-hashtags
  ([task-name task-opts] (window-trending-hashtags
                          task-name :twit.persist.sql/upsert-trending task-opts))
  ([task-name sync-fn task-opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/type :function
                             :onyx/group-by-key :hashtag
                             :onyx/flux-policy :continue
                             :onyx/uniqueness-key :id
                             :onyx/min-peers 1
                             :onyx/max-peers 1
                             :onyx/fn :clojure.core/identity}
                            task-opts)
           :windows [{:window/id (keyword (str task-name "-" "window"))
                      :window/task task-name
                      :window/type :global
                      :window/window-key :created-at
                      :window/aggregation :onyx.windowing.aggregation/count}]
           :triggers [{:trigger/window-id (keyword (str task-name "-" "window"))
                       :trigger/refinement :onyx.refinements/accumulating
                       :trigger/on :onyx.triggers/segment
                       :trigger/threshold [10 :elements]
                       :trigger/sync :twit.persist.sql/upsert-trending}]}}))

(defn with-counting-hashtags
  [window-id]
  (fn [task-definition]
    (let [task-name (get-in task-definition [:task :task-map :onyx/name])]
      (-> task-definition
          (assoc-in [:task :task-map :onyx/group-by-key] :hashtag)
          (assoc-in [:task :task-map :onyx/flux-policy] :continue)
          (assoc-in [:task :task-map :onyx/uniqueness-key] :id)
          (update-in [:task :windows] conj {:window/id window-id
                                            :window/task task-name
                                            :window/type :global
                                            :window/window-key :created-at
                                            :window/aggregation :onyx.windowing.aggregation/count})))))

(defn with-syncing-to-sql
  [window-id]
  (fn [task-definition]
    (-> task-definition
        (update-in [:task :triggers] conj {:trigger/window-id window-id
                                           :trigger/refinement :onyx.refinements/accumulating
                                           :trigger/on :onyx.triggers/segment
                                           :trigger/threshold [10 :elements]
                                           :trigger/sync :twit.persist.sql/upsert-trending}))))
