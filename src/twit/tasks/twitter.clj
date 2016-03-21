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

(defn ff []
  {:task {:task-map {:onyx/name :_notused
                     :onyx/type :function
                     :onyx/fn :clojure.core/identity
                     :onyx/batch-size 1
                     :onyx/batch-timeout 1}}})
