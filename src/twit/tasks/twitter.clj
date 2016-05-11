(ns twit.tasks.twitter
  (:require [schema.core :as s]
            [twit.persist.sql :refer [upsert-emojicount]]))

(defn count-emojis
  [keypath resultpath segment]
  (let [emoji-count
        (reduce (fn [acc char]
                  (if (<= (int Character/MIN_SURROGATE) (int char) (int Character/MAX_SURROGATE))
                    (inc acc)
                    acc)) 0 (get-in segment keypath))]
    (assoc-in segment resultpath emoji-count)))

(defn add-emoji-count
  "Counts the emojis located at the emoji-string path and assoc's the counts to the result-path"
  [task-name keypath resultpath task-opts]
  {:task {:task-map (merge {:onyx/name task-name
                            :onyx/type :function
                            :onyx/fn ::count-emojis
                            ::emoji-string keypath
                            ::result-path resultpath
                            :onyx/params [::emoji-string ::result-path]}
                           task-opts)}
   :schema {:task-map {::emoji-string [s/Any]
                       ::result-path [s/Any]}}})

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

(defn window-emojiscore-by-country
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
  ([task-name task-opts]
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
                       :trigger/threshold [20 :elements]
                       :trigger/sync :twit.persist.sql/upsert-trending}]}}))
