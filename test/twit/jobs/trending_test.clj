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
            [joplin.jdbc.database]
            [onyx.api]))

(deftest trending-test
  (testing "We can get trending view"
    (let [{:keys [env-config
                  peer-config
                  twitter-config
                  joplin-config]} (read-config (io/resource "config.edn"))
          job (twit.jobs.trending/build-job twitter-config joplin-config 1 1000)
          {:keys [in out]} (get-core-async-channels job)]
      (with-test-env [test-env [10 env-config peer-config]]
        (onyx.test-helper/validate-enough-peers! test-env job)
        (onyx.api/submit-job peer-config job)
        (loop [tweet (<!! out)]
          (recur (<!! out)))
        (is (<!! out))))))
