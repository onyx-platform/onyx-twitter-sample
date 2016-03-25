(ns twit.persist.db
  "Saving stuff"
  (:require [clojure.java.jdbc :as j]
            [aero.core :refer [read-config]]
            [joplin.jdbc.database :as d]
            [joplin.core :as joplin]
            [taoensso.timbre :refer [info warn]]
            [honeysql.core :as sql]
            [honeysql.helpers :as helpers]
            [clojure.java.io :as io]))


(defn dev
  "Trigger sync function. state event and state contain information about the
   triggered window extant. We take the unix timestamp timerange, convert it to hh:mm:ss,
   and store that to make it easier to read what happened."
  [event window trigger {:keys [group-key trigger-update] :as state-event} state]
  (let [sql-map {:connection-uri
                 "jdbc:mysql://192.168.99.100:3306/onyx?user=admin&password=mypass"}]
    (when (and (:average state)
               (not (zero? (:average state))))
      (j/execute! sql-map (-> (helpers/insert-into :EmojiRank)
                              (helpers/values [{:CountryCode group-key
                                                :TotalTweets (:sum state)
                                                :timespan (:lower-bound state-event)}])
                              (sql/format {:quoting :mysql}))))))

(defn sql [event window trigger {:keys [group-key trigger-update] :as state-event} state])

(defn no-pending-migrations? [event lifecycle]
  (let [joplin-key :joplin-config
        joplin-config (or (get-in lifecycle [joplin-key])
                          (get-in event [:onyx.core/task-map joplin-key])
                          (throw (Exception. (str joplin-key " not specified"))))
        joplin-db-env (or (get-in lifecycle [:joplin-environment]) ;; select enviornment profile
                          (get-in event [:onyx.core/task-map :joplin-environment]))]
    (mapv (fn [env]
            (joplin/migrate-db env))
          (get-in joplin-config [:environments joplin-db-env]))
    (if (every? nil? (mapv (fn [env]
                             (joplin/pending-migrations env))
                           (get-in joplin-config [:environments joplin-db-env])))
      (do (info "Migrations successful")
          true)
      (do (warn "Migrations unsuccessful, retrying")
          false))))

(def joplin
  {:lifecycle/start-task? no-pending-migrations?})

#_(joplin/migrate-db
   {:db {:type :sql,
         :url "datomic:mem://test"}
    :migrator "joplin/migrators/datomic"
    :seed "seeds.dt/run"})

#_(j/insert!
 {:connection-uri
  "jdbc:mysql://192.168.99.100:3306/onyx?user=admin&password=mypass"}
 ["INSERT INTO EmojiRank (CountryCode, TotalTweets, timespan) VALUES (?, 12, 1458936790000)" "FR"])
