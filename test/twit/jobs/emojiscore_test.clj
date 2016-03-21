(ns twit.jobs.emojiscore-test
  (:require [aero.core :refer [read-config]]
            [twit.jobs.emojiscore]
            [onyx.plugin.core-async :refer [get-core-async-channels
                                            take-segments!]]
            [onyx.plugin.twitter]
            [clojure.core.async :refer [>!! <!!]]
            [onyx.test-helper :refer [with-test-env]]
            [clojure.test :refer [is testing deftest]]
            [clojure.java.io :as io]
            [onyx.api]))

(deftest basic-test
  (testing "That we can have a basic in-out workflow run through Onyx"
    (let [{:keys [env-config
                  peer-config
                  twitter-config]} (read-config (io/resource "config.edn"))
          job (twit.jobs.emojiscore/build-job twitter-config 10 1000)
          {:keys [in out]} (get-core-async-channels job)]
      (with-test-env [test-env [3 env-config peer-config]]
        (onyx.test-helper/validate-enough-peers! test-env job)
        (onyx.api/submit-job peer-config job)
        (println (<!! out))
        (is (<!! out))))))
