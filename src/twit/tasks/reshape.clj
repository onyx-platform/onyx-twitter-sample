(ns twit.tasks.reshape
  (:require [clojure.walk :refer [postwalk]]
            [schema.core :as s]
            [taoensso.timbre :refer [info]]))

(defn transform-shape
  "Recursively restructures a segment {:new-key [paths...]}"
  [paths segment]
  (try
    (let [f (fn [[k v]]
              (if (vector? v)
                [k (get-in segment v)]
                [k v]))]
      (postwalk (fn [x] (if (map? x) (into {} (map f x)) x)) paths))
    (catch Exception e
      (do (info "Could not transform segment: " segment)
          {}))))

(defn reshape-segment
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
