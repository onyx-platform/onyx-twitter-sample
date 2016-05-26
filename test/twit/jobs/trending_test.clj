(ns twit.jobs.trending-test
  (:require [aero.core :refer [read-config]]
            [clojure.core.async :refer [pipe]]
            [clojure.core.async.lab :refer [spool]]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [onyx api
             [test-helper :refer [with-test-env]]]
            [onyx.plugin.core-async :refer [get-core-async-channels take-segments!]]
            [onyx.plugin.twitter]
            [lib-onyx.joplin]
            [joplin.jdbc.database]
            twit.jobs.trending
            [twit.persist.atom :as p-atom]))

(def window-range [Double/NEGATIVE_INFINITY Double/POSITIVE_INFINITY])

(def input
  (conj (mapv (fn [id]
                {:id id :text "Hello #world"
                 :createdAt (java.util.Date.)}) (range 10))
        (mapv (fn [id]
                {:id id :text "Not #trending"
                 :createdAt (java.util.Date.)}) (range 1))
        :done))

(deftest trending-test
  (testing "We can get trending view"
    (let [{:keys [env-config peer-config]} (read-config (io/resource "config.edn"))
          batch-settings {:onyx/batch-size 1
                          :onyx/batch-timeout 1000}
          job (-> (twit.jobs.trending/trending-hashtags-job batch-settings)
                  (twit.jobs.trending/add-test-leafs batch-settings))
          {:keys [in out]} (get-core-async-channels job)
          {:keys [test-atom]} (p-atom/get-stores job)]
      (with-test-env [test-env [10 env-config peer-config]]
        (pipe (spool input) in)
        (onyx.test-helper/validate-enough-peers! test-env job)
        (onyx.api/submit-job peer-config job)
        (take-segments! out)
        (is (= 10 (get-in @test-atom [window-range "#world"])))
        (is (nil? (get-in @test-atom [window-range "#trending"])))))))

#_(let [{:keys [env-config peer-config twitter-config joplin-config]} (read-config (io/resource "config.edn"))
      batch-settings {:onyx/batch-size 1
                      :onyx/batch-timeout 1000}
      job (-> (twit.jobs.trending/trending-hashtags-job batch-settings)
              (twit.jobs.trending/add-prod-leafs twitter-config joplin-config batch-settings))
      {:keys [out]} (get-core-async-channels job)]
  (with-test-env [test-env [10 env-config peer-config]]
    (onyx.test-helper/validate-enough-peers! test-env job)
    (onyx.api/submit-job peer-config job)
    (take-segments! out)))
