(ns twit.jobs.smoke-test
  (:require [aero.core :refer [read-config]]
            [clojure.core.async :refer [>!!]]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [onyx api
             [test-helper :refer [with-test-env]]]
            [onyx.plugin.core-async :refer [get-core-async-channels take-segments!]]
            [onyx.plugin.seq]
            twit.jobs.smoke
            ;; Include function definitions
            [twit.tasks.math]
            [twit.tasks.logging :refer [with-segment-logging]]
            [onyx.tasks.core-async]))

(deftest smoke-test
  (testing "Test"
    (let [{:keys [env-config
                  peer-config]} (read-config (io/resource "config.edn"))
          job (twit.jobs.smoke/smoketest-job {:onyx/batch-size 10
                                              :onyx/batch-timeout 1000})
          {:keys [out]} (get-core-async-channels job)]
      (with-test-env [test-env [3 env-config peer-config]]
        (onyx.test-helper/validate-enough-peers! test-env job)
        (onyx.api/submit-job peer-config job)
        (is (not (empty? (set (take-segments! out)))))))))
