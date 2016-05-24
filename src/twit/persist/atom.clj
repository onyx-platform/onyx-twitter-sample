(ns twit.persist.atom
  (:require [clojure.set :refer [join]]))

;; Place to store our atom's
(defonce dbs (atom {}))

(defn get-store
  "Fetches a db by id, creates one if there is not one."
  [id]
  (if-let [db (get @dbs id)]
    db
    (get (swap! dbs assoc id (atom {})) id)))

(defn get-stores
  "Walks through triggers with a :testing-trigger/db-atom present,
  returns a map of trigger :sync atoms."
  [{:keys [catalog windows triggers]}]
  (reduce (fn [acc itm]
            (if-let [id (:twit.persist.atom/atom-id itm)]
              (assoc acc id (get-store id))
              acc)) {} triggers))

(defn inject-store
  [_ lifecycle]
  {:twit.persist.atom/store (get-store (:twit.persist.atom/atom-id lifecycle))})

(def calls
  {:lifecycle/before-task-start inject-store})

(defn persist-trending
  [event window trigger {:keys [group-key trigger-update] :as state-event} state]
  (let [store-id (get trigger :twit.persist.atom/atom-id)
        store (get-store store-id)]
    (swap! store assoc-in [[(:lower-bound state-event) (:upper-bound state-event)]
                           (or group-key "none")] state)))
