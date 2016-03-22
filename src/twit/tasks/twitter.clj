(ns twit.tasks.twitter
  (:import [java.text SimpleDateFormat]))

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

;;(:event-type :task-event :segment :grouped? :group-key
;; :lower-bound :upper-bound :log-type :trigger-update :aggregation-update
;; :window :next-state :extents :extent :trigger-index :trigger-state :extent-state)

(defn to-stdout [event window trigger
                 {:keys [group-key trigger-update] :as state-event}
                 state]
  (let [date-formatter (fn [time] (.format (new SimpleDateFormat "hh:mm:ss")
                                           (java.util.Date. time)))]

    (when (and (:average state)
               (not (zero? (:average state))))

      (swap! aa assoc-in [(map date-formatter
                               [(:lower-bound state-event)
                                (:upper-bound state-event)]) group-key] {:emojis-per-tweet (long (:average state))
                                                                         :number-of-tweets (:n state)}))))

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
                     :window/aggregation [:onyx.windowing.aggregation/average emoji-key]
                     :window/range [1 :minutes]}]
          :triggers [{:trigger/window-id (keyword (str task-name "-" "window"))
                      :trigger/refinement :onyx.refinements/accumulating
                      :trigger/on :onyx.triggers/watermark
                      :trigger/fire-all-extents? true
                      :trigger/sync ::to-stdout}]}})
