(ns twit.tasks.segment
  (:require [clojure.walk :refer [postwalk]]))

(defn transform-shape
  "Recursively restructures a segment {:new-key [paths...]}"
  [paths segment]
  (try (let [f (fn [[k v]]
                 (if (vector? v)
                   [k (get-in segment v)]
                   [k v]))]
         (postwalk (fn [x] (if (map? x) (into {} (map f x)) x)) paths))
       (catch Exception e
         segment)))

(defn transform-segment-shape
  "Recursively restructures a segment {:new-key [paths...]}"
  [task-name paths task-opts]
  {:task {:task-map (merge {:onyx/name task-name
                            :onyx/type :function
                            :onyx/fn ::transform-shape
                            ::paths paths
                            :onyx/params [::paths]}
                           task-opts)}})

(defn filter-keypath-pred [event old-segment new-segment all-new keypath]
  (get-in new-segment keypath nil))

(defn filter-keypath
  "Filter out segments where keypath is either nil or does not exist"
  [from to keypath]
  {:task {:task-map {:onyx/name :_notused
                     :onyx/type :function
                     :onyx/fn :clojure.core/identity
                     :onyx/batch-size 1
                     :onyx/batch-timeout 1}
          :flow-conditions [{:flow/from from
                             :flow/to to
                             :flow/short-circuit? (or (= :all to)
                                                      (= :none to))
                             ::keypath keypath
                             :flow/predicate [::filter-keypath-pred ::keypath]}]}})
