(ns twit.persist.sql
  (:require [clojure.java.jdbc :as jdbc]))

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

(defn upsert-emojicount
  [event window trigger {:keys [group-key trigger-update] :as state-event} state]
  (let [joplin-env (get-in event [:onyx.core/task-map :joplin/environment])
        joplin-uri (get-in event [:onyx.core/task-map :joplin/config :environments joplin-env 0 :db :url])]
    (let [row {:CountryCode (or group-key "Unknown")
               :TotalTweets (int (:n state))
               :timespan (str (:lower-bound state-event) " - " (:upper-bound state-event))
               :AverageEmojis (int (:average state))}]
      (upsert! {:connection-uri joplin-uri}
               :EmojiRank
               row
               {:TotalTweets   (int (:n state))
                :AverageEmojis (int (:average state))}))))

(defn upsert-trending
  [event window trigger {:keys [group-key trigger-update] :as state-event} state]
  (let [joplin-env (get-in event [:onyx.core/task-map :joplin/environment])
        joplin-uri (get-in event [:onyx.core/task-map :joplin/config :environments joplin-env 0 :db :url])]
    (let [row {:hashtag (or group-key "none")
               :score state
               :timespan (str (:lower-bound state-event) " - " (:upper-bound state-event))}]
      (upsert! {:connection-uri joplin-uri}
               :Trending
               row
               {:score state
                ;:timespan (str (:lower-bound state-event) " - " (:upper-bound state-event))
                }))))
