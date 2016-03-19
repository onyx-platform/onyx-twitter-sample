(ns twit.jobs.basic-test.clj
  (:require [aero.core :refer [read-config]]
            [twit.jobs.basic-test]
            [onyx.plugin.core-async :refer [get-core-async-channels
                                            take-segments!]]
            [clojure.core.async :refer [>!!]]
            [onyx.test-helper :refer [with-test-env]]
            [clojure.test :refer [is testing deftest]]
            [clojure.java.io :as io]))

(def segments [{:n 1}
               {:n 2}
               {:n 3}
               {:n 4}
               {:n 5}
               :done])

(deftest basic-test
  (testing "That we can have a basic in-out workflow run through Onyx"
    (let [{:keys [env-config
                  peer-config]} (read-config (io/resource "config.edn"))
          job (twit.jobs.basic/build-job 10 1000)
          {:keys [in out]} (get-core-async-channels job)
          ])))
