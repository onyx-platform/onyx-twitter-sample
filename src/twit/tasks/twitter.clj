(ns twit.tasks.twitter
  (:require [schema.core :as s]
            [onyx.schema :as os]
            [clojure.java.jdbc :as j]
            [taoensso.timbre :refer [info warn]]
            [honeysql.core :as sql]
            [honeysql.helpers :as helpers]
            [clojure.java.io :as io]
            [schema.core :as s]
            [clojure.java.jdbc :as jdbc])
  (:import [java.text SimpleDateFormat]))

(defn upsert-sql
  [table columns updates]
  (let [updates (map (fn [[column value]]
                       (str (name column) " = " value))
                     updates)]
    (str "INSERT INTO " (name table) " "
         "(" (clojure.string/join ", " (map name columns)) ") "
         "VALUES (" (clojure.string/join ", " (repeat (count columns) "?")) ") "
         (when (seq updates)
           (str "ON DUPLICATE KEY UPDATE "
                (clojure.string/join ", " updates))))))

(defn upsert!
  "Some function I got online to do mysql upserts"
  ([db table values updates]
   (upsert! db table (keys values) [(vals values)] updates))
  ([db table columns rows updates]
   (when (seq rows)
     (let [sql (upsert-sql table columns updates)]
       (apply jdbc/db-do-prepared db false sql rows)))))

(defn sync-to-sql
  [event window trigger {:keys [group-key trigger-update] :as state-event} state]
  (let [joplin-env (get-in event [:onyx.core/task-map :joplin/environment])
        joplin-uri (get-in event [:onyx.core/task-map :joplin/config :environments joplin-env 0 :db :url])]
    (assert joplin-uri "Could not find a joplin-url in the task map")
    (let [row {:CountryCode (or group-key "Unknown")
               :TotalTweets (int (:n state))
               :timespan (str (:lower-bound state-event) " - " (:upper-bound state-event))
               :AverageEmojis (int (:average state))}]
      (upsert! {:connection-uri joplin-uri}
               :EmojiRank
               row
               {:TotalTweets   (int (:n state))
                :AverageEmojis (int (:average state))}))))

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
                       :trigger/threshold [15 :elements]
                       :trigger/sync ::sync-to-sql}]}}))
