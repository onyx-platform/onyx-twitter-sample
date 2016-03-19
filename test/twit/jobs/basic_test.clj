(ns twit.jobs.basic-test.clj
  (:require [aero.core :refer [read-config]]
            [twit.jobs.basic-test]
            [onyx.plugin.core-async :refer [get-core-async-channels
                                            take-segments!]]
            [clojure.core.async :refer [>!!]]
            [onyx.test-helper :refer [with-test-env]]
            [clojure.test :refer [is testing deftest]]))

(def segments [{:n 1}
               {:n 2}
               {:n 3}
               {:n 4}
               {:n 5}
               :done])
