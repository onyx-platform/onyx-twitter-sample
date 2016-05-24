(ns twit.jobs.trending-test
  (:require [aero.core :refer [read-config]]
            [twit.jobs.trending]
            [onyx.plugin.core-async :refer [get-core-async-channels
                                            take-segments!]]
            [onyx.plugin.twitter]
            [clojure.core.async :refer [>!! <!!]]
            [onyx.test-helper :refer [with-test-env]]
            [clojure.test :refer [is testing deftest]]
            [clojure.java.io :as io]
            [clojure.core.async.lab :refer [spool]]
            [clojure.core.async :refer [pipe]]
            [twit.persist.atom :as p-atom]
            [onyx.api]))

(def window-range [Double/NEGATIVE_INFINITY Double/POSITIVE_INFINITY])

(def input
  (conj (mapv (fn [id]
                {:id id :text "Hello #world"
                 :createdAt (java.util.Date.)}) (range 10)) :done))

(deftest trending-test
  (testing "We can get trending view"
    (let [{:keys [env-config peer-config twitter-config joplin-config]} (read-config (io/resource "config.edn"))
          batch-settings {:onyx/batch-size 1
                          :onyx/batch-timeout 1000}
          job (-> (twit.jobs.trending/trending-hashtags-job batch-settings)
                  (twit.jobs.trending/add-test-leafs twitter-config batch-settings))
          {:keys [in out]} (get-core-async-channels job)
          {:keys [test-atom]} (p-atom/get-stores job)]
      (with-test-env [test-env [10 env-config peer-config]]
        (pipe (spool input) in)
        (onyx.test-helper/validate-enough-peers! test-env job)
        (onyx.api/submit-job peer-config job)
        (take-segments! out)
        (is (= 10 (get-in @test-atom [window-range "#world"])))))))
