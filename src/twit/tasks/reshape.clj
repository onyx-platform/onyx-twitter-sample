(ns twit.tasks.reshape
  (:require [clojure.walk :refer [postwalk]]
            [schema.core :as s]))

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
  "Recursively restructure a segment, like select-keys but with get-in style
  key paths.

  {:name [:user :name]}
  applied to
  {:user {:name 'gardner'}}
  would result in
  {:name 'gardner'}"
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
  "Filters segments that have a value (located at keypath) nil for
  a specific graph edge (from->to)"
  [from to keypath]
  {:task {:flow-conditions [{:flow/from from
                             :flow/to to
                             :flow/short-circuit? (or (= :all to)
                                                      (= :none to))
                             ::keypath keypath
                             :flow/predicate [::filter-keypath-pred ::keypath]}]}
   :schema {:flow-conditions [{::keypath [s/Any]}]}})
