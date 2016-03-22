(ns twit.tasks.twitter)

(defn count-emojis
  ""
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

(def aa (atom {}))

(defn to-stdout [event window trigger
                 {:keys [group-key trigger-update] :as state-event}
                 state]
  (when (and state
            (not (zero? state)))
    (swap! aa assoc group-key state)))

(defonce f (future (loop []
                     (clojure.pprint/pprint @aa)
                     (Thread/sleep 30000)
                     (recur))))

(defn emojiscore-by-country [task-name emoji-key task-opts]
  {:task {:task-map (merge {:onyx/name task-name
                            :onyx/type :function
                            :onyx/group-by-key :country
                            :onyx/flux-policy :kill
                            :onyx/min-peers 1
                            :onyx/max-peers 1
                            :onyx/fn :clojure.core/identity}
                           task-opts)
          :windows [{:window/id (keyword (str task-name "-" "window"))
                     :window/task task-name
                     :window/type :fixed
                     :window/window-key :created-at
                     :window/aggregation [:onyx.windowing.aggregation/sum emoji-key]
                     :window/range [30 :seconds]}]
          :triggers [{:trigger/window-id (keyword (str task-name "-" "window"))
                      :trigger/refinement :onyx.refinements/discarding
                      :trigger/on :onyx.triggers/watermark
                      :trigger/fire-all-extents? true
                      :trigger/sync ::to-stdout}]}})
