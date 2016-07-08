(ns twit.tasks.logging
  (:require [taoensso.timbre :refer [info]]))

(defn with-logging []
  )

(defn log-batch [event lifecycle]
  (let [task-name (:onyx/name (:onyx.core/task-map event))]
    (doseq [m (map :message (mapcat :leaves (:tree (:onyx.core/results event))))]
      (info task-name "Logging segment:" m)))
  {})

(def log-calls
  {:lifecycle/after-batch log-batch})

(defn with-segment-logging
  []
  (fn [task-definition]
    (let [task-name (get-in task-definition [:task :task-map :onyx/name])]
      (-> task-definition
          (update-in [:task :lifecycles] conj
                     {:lifecycle/task task-name
                      :lifecycle/calls ::log-calls})))))
