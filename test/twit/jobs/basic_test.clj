(ns twit.jobs.basic-test.clj
  (:require [aero.core :refer [read-config]]
            [twit.jobs.basic-test]
            [onyx.plugin.core-async :refer [get-core-async-channels]]
            [onyx.test-helper :refer [with-test-env]]))
