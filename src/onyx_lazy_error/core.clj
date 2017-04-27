(ns onyx-lazy-error.core
  (:require [onyx-lazy-error.utils :as u]))

(def workflow
  [[:read-segments :generate-seq]
   [:generate-seq  :use-seq]
   [:use-seq       :write-segments]
   [:generate-seq  :write-segments]])


(def flow-conditions
  [{:flow/from              :generate-seq
    :flow/to                [:write-segments]
    :flow/short-circuit?    true
    :flow/thrown-exception? true
    :flow/predicate         ::constantly-true
    :flow/post-transform    ::post-transform}

   {:flow/from              :use-seq
    :flow/to                [:write-segments]
    :flow/short-circuit?    true
    :flow/thrown-exception? true
    :flow/predicate         ::constantly-true
    :flow/post-transform    ::post-transform}])


(defn build-catalog []
  (let [batch-size    5
        batch-timeout 50]
    [{:onyx/name          :read-segments
      :onyx/plugin        :onyx.plugin.core-async/input
      :onyx/type          :input
      :onyx/medium        :core.async
      :onyx/batch-size    batch-size
      :onyx/batch-timeout batch-timeout
      :onyx/max-peers     1}

     {:onyx/name          :generate-seq
      :onyx/fn            ::generate-seq
      :onyx/type          :function
      :onyx/batch-size    batch-size
      :onyx/batch-timeout batch-timeout}

     {:onyx/name          :use-seq
      :onyx/fn            ::use-seq
      :onyx/type          :function
      :onyx/batch-size    batch-size
      :onyx/batch-timeout batch-timeout}

     {:onyx/name          :write-segments
      :onyx/plugin        :onyx.plugin.core-async/output
      :onyx/type          :output
      :onyx/medium        :core.async
      :onyx/batch-size    batch-size
      :onyx/batch-timeout batch-timeout
      :onyx/max-peers     1}]))


(defn inject-writer-ch [event lifecycle]
  {:core.async/chan (u/get-output-channel (:core.async/id lifecycle))})


(def writer-lifecycle
  {:lifecycle/before-task-start inject-writer-ch})


(defn build-lifecycles []
  [{:lifecycle/task  :read-segments
    :lifecycle/calls ::u/in-calls
    :core.async/id   (java.util.UUID/randomUUID)}

   {:lifecycle/task  :read-segments
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}

   {:lifecycle/task  :write-segments
    :lifecycle/calls ::writer-lifecycle
    :core.async/id   (java.util.UUID/randomUUID)}

   {:lifecycle/task  :write-segments
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])


;; fns


(defn generate-seq [{:keys [n]}]
  {:sequence (->> (for [i (range n)]
                    (do (throw (Exception. "Blow it away"))
                        i))
                  ;; uncomment this for tests to pass
                  #_(into []))})


(defn use-seq [{:keys [sequence]}]
  {:sum (reduce + sequence)})


(defn constantly-true [_ _ _ _]
  (constantly true))


(defn post-transform [event segment e]
  (let [task   (-> event :onyx.core/task-map :onyx/name)
        result {:task-name task :exception e}]
    ;; (println "GOTCHA" result)
    result))
