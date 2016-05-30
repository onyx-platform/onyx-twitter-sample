(ns twit.jobs)

(defmulti register-job (fn [job-name config] job-name))
