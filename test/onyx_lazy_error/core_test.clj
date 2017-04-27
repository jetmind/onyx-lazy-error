(ns onyx-lazy-error.core-test
  (:require [clojure.test :refer :all]
            [onyx.test-helper :refer [with-test-env feedback-exception!]]
            [onyx-lazy-error.core :as c]
            [onyx-lazy-error.utils :as u]))


(def zk-port 2188)
(def zk-str (str "127.0.0.1:" zk-port))


(defn peer-config [id]
  {:onyx.peer/job-scheduler  :onyx.job-scheduler/balanced
   :onyx.messaging/impl      :aeron
   :onyx.messaging/peer-port 40200
   :onyx.messaging/bind-addr "localhost"
   :zookeeper/address        "127.0.0.1:2188"
   :onyx/tenancy-id          id})


(defn env-config [id]
  {:zookeeper/server?                   true
   :onyx/tenancy-id                     id
   :zookeeper/address                   zk-str
   :zookeeper.server/port               zk-port
   :onyx.bookkeeper/server?             true
   :onyx.bookkeeper/delete-server-data? true
   :onyx.bookkeeper/local-quorum?       true
   :onyx.bookkeeper/local-quorum-ports  [3196 3197 3198]})


(def input
  [{:n 10}
   :done])


(defn find-result-segment [results]
  (first (filter :task-name results)))


(deftest test-lazy-sequence-realization
  (let [cluster-id  (java.util.UUID/randomUUID)
        env-config  (env-config cluster-id)
        peer-config (peer-config cluster-id)
        catalog     (c/build-catalog)
        lifecycles  (c/build-lifecycles)
        n-peers     (u/n-peers catalog c/workflow)]
    (with-test-env
      [test-env [n-peers env-config peer-config]]
      (u/bind-inputs! lifecycles {:read-segments input})
      (let [job    {:workflow        c/workflow
                    :catalog         catalog
                    :lifecycles      lifecycles
                    :flow-conditions c/flow-conditions
                    :task-scheduler  :onyx.task-scheduler/balanced}
            job-id (:job-id (onyx.api/submit-job peer-config job))]
        (assert job-id "Job was not successfully submitted")
        (feedback-exception! peer-config job-id)
        (let [[results]      (u/collect-outputs! lifecycles [:write-segments])
              result         (find-result-segment results)]

          ;; Expecting to see :generate-seq in the :task-name here, since onyx
          ;; serializes segment to send it over the network, but seeing
          ;; downstream task (:use-seq) which indicates lazy seq realized after
          ;; it was passed to the next task

          ;; Also you can see it in the stack trace:
          ;; (println (:exception result))

          (is (= (:task-name result) :generate-seq)))))))
