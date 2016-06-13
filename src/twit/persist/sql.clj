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

(defn upsert-trending
  [event window trigger {:keys [group-key trigger-update] :as state-event} state]
  (let [{:keys [sql/connection-uri]} trigger
        row {:hashtag (or group-key "none")
             :score state
             :timespan (str (:lower-bound state-event) " - " (:upper-bound state-event))}]
    (when-not connection-uri (throw (IllegalArgumentException. "connection-uri not specified")))
    (upsert! connection-uri :Trending row {:score state})))
