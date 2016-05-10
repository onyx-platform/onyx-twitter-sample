(ns twit.tasks.twitter
  (:require [schema.core :as s]
            [onyx.schema :as os]
            [clojure.java.jdbc :as j]
            [taoensso.timbre :refer [info warn]]
            [honeysql.core :as sql]
            [honeysql.helpers :as helpers]
            [clojure.java.io :as io]
            [schema.core :as s])
  (:import [java.text SimpleDateFormat]))

(defn dev
  [event window trigger {:keys [group-key trigger-update] :as state-event} state]
  (let [joplin-env (get-in event [:onyx.core/task-map :joplin/environment])
        joplin-uri (get-in event [:onyx.core/task-map :joplin/config :environments joplin-env 0 :db :url])]
    (assert joplin-uri "Could not find a joplin-url in the task map")
    (when (and (:average state)
               (not (zero? (:average state))))
      (j/execute! {:connection-uri joplin-uri} (-> (helpers/insert-into :EmojiRank)
                              (helpers/values [{:CountryCode group-key
                                                :TotalTweets (:sum state)
                                                :timespan (:lower-bound state-event)
                                                :AverageEmojis (:average state)}])
                              (sql/format {:quoting :mysql}))))))

(defn count-emojis
  [keypath resultpath segment]
  (let [emoji-count (reduce + (map (fn [char]
                                     (if (<= (int Character/MIN_SURROGATE)
                                             (int char) (int Character/MAX_SURROGATE))
                                       1
                                       0))
                                   (get-in segment keypath)))]
    (assoc-in segment resultpath emoji-count)))

(defn add-emoji-count
  "Counts the emojis located at the keypath and assoc's the counts to the resultpath"
  [task-name keypath resultpath task-opts]
  {:task {:task-map (merge {:onyx/name task-name
                            :onyx/type :function
                            :onyx/fn ::count-emojis
                            ::keypath keypath
                            ::resultpath resultpath
                            :onyx/params [::keypath ::resultpath]}
                           task-opts)}})

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
                      :window/type :fixed
                      :window/window-key :created-at
                      :window/aggregation [:onyx.windowing.aggregation/average :emoji-count]
                      :window/range [10 :seconds]}]
           :triggers [{:trigger/window-id (keyword (str task-name "-" "window"))
                       :trigger/refinement :onyx.refinements/discarding
                       :trigger/on :onyx.triggers/watermark
                       :trigger/sync ::dev}]}}))
