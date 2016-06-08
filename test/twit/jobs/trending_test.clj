(ns twit.jobs.trending-test
  (:require [aero.core :refer [read-config]]
            [clojure.test :refer [deftest is testing]]
            [clojure.core.async :refer [pipe]]
            [clojure.core.async.lab :refer [spool]]
            [lib-onyx.persist.atom :as p-atom]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            twit.jobs.trending
            ;;; Include function definitions
            twit.tasks.reshape
            twit.tasks.twitter
            lib-onyx.persist.atom))

(def window-range [Double/NEGATIVE_INFINITY Double/POSITIVE_INFINITY])

(def input
  (conj (mapv (fn [id]
                {:id id :text "Hello #world"
                 :createdAt (java.util.Date.)}) (range 10))
        :done))

(defn test-job
  "Picking up where trending-hashtags-job left off, we add the root and 'leafs'
  to the graph for a testing environment. This is a core-async input and output
  channel, and a plain old atom as our :trigger/sync target."
  [job batch-settings]
  (let [aggregation-settings
        {:onyx/group-by-key :hashtag
         :onyx/flux-policy :recover
         :onyx/min-peers 1
         :onyx/max-peers 1
         :onyx/uniqueness-key :id}]
    (-> job
        (add-task (onyx.tasks.core-async/input :in batch-settings))
        (add-task (onyx.tasks.core-async/output :out (merge batch-settings aggregation-settings))
                  (twit.tasks.twitter/with-trigger-to-atom :hashtag-window :test-atom)))))

(deftest trending-test
  (testing "We can get trending view"
    (let [{:keys [env-config peer-config]} (read-config (clojure.java.io/resource "config.edn"))
          batch-settings {:onyx/batch-size 1
                          :onyx/batch-timeout 1000}
          job (-> (twit.jobs.trending/trending-hashtags-job batch-settings)
                  (test-job batch-settings))
          {:keys [in out]} (onyx.plugin.core-async/get-core-async-channels job)
          {:keys [test-atom]} (p-atom/get-stores job)]
      (with-test-env [test-env [10 env-config peer-config]]
        (pipe (spool input) in)
        (onyx.test-helper/validate-enough-peers! test-env job)
        (onyx.api/submit-job peer-config job)
        (onyx.plugin.core-async/take-segments! out)
        (is (= 10 (get-in @test-atom [window-range "#world"])))))))
