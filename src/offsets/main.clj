(ns offsets.main
  (:gen-class)
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.pprint :as pprint]
            [clojure.tools.cli :as cli])
  (:import (org.apache.kafka.clients.admin AdminClient
                                           AdminClientConfig
                                           ListConsumerGroupOffsetsOptions
                                           OffsetSpec)
           (org.apache.kafka.common TopicPartition KafkaException)
           (org.apache.kafka.clients.consumer OffsetAndMetadata)))

(def ^:dynamic kafka-config)
(def app-name "offsets")

(def actions {:topic-offsets {:required-args #{:topic}
                              :description "Print offsets for topic, earliest and latest"}
              :group-offsets {:required-args #{:topic :group}
                              :description "Print offsets of consumer group for topic"}
              :reset-offsets {:required-args #{:topic :group :strategy}
                              :description "Reset offsets for consumer group for topic"}})

(defn usage [summary]
  (str/join \newline
            [(str app-name ": Modify and show Kafka topic and consumer group offsets")
             (str "Usage: " app-name " <profile> <action> [options]")
             ""
             "Options:"
             summary
             ""
             "Profiles:"
             (str "\t" (str/join ", " (map name (keys kafka-config))))
             ""
             "Actions:"
             (str/join \newline (map (fn [[k v]] (format "\t%s\t%s" (name k) (:description v))) actions))]))

(defn error
  ([errors summary]
   (str/join \newline
             [(str "Error: " errors)
              ""
              (usage summary)]))
  ([errors]
   (str "Error: " errors)))

(defn exit [msg status]
  (println msg)
  (System/exit status))

(defn client [config]
  (AdminClient/create {AdminClientConfig/BOOTSTRAP_SERVERS_CONFIG (:broker-url config)
                       AdminClientConfig/SECURITY_PROTOCOL_CONFIG "SASL_SSL"
                       "sasl.mechanism" "PLAIN"
                       "sasl.jaas.config"
                       (format "org.apache.kafka.common.security.plain.PlainLoginModule required\nusername='%s' password='%s';"
                               (:username config)
                               (:password config))}))


(defn partition-count [client topic]
  (-> (.describeTopics client [topic])
      .all
      .get
      (get topic)
      .partitions
      count))

(defn all-partitions-for-topic [client topic]
  (map #(TopicPartition. topic %) (range (partition-count client topic))))

(defn get-group-offsets [client topic group]
  (let [options (-> (ListConsumerGroupOffsetsOptions.)
                    (.topicPartitions (all-partitions-for-topic client topic)))]
    (-> (.listConsumerGroupOffsets client group options)
        .partitionsToOffsetAndMetadata
        .get)))

(defn get-topic-offsets [client topic strategy]
  (let [strategy (condp = strategy
                   :latest (OffsetSpec/latest)
                   :earliest (OffsetSpec/earliest))
        topicspec (into {} (map #(vector % strategy) (all-partitions-for-topic client topic)))]
    (-> (.listOffsets client topicspec)
        .all
        .get)))

(defn print-topic-offsets [client topic]
  (let [earliest (get-topic-offsets client topic :earliest)
        latest (get-topic-offsets client topic :latest)]
    (pprint/print-table (map (fn [[k1 earliest] [k2 latest]]
                               {"Topic" (.topic k1)
                                "Partition" (.partition k1)
                                "Earliest" (.offset earliest)
                                "Latest" (.offset latest)})
                             earliest latest))))

(defn print-group-offsets [client topic group]
  (pprint/print-table (map (fn [[k v]]
                             {"Topic" (.topic k)
                              "Partition" (.partition k)
                              "Offset" (if (nil? v) "unset" (.offset v))})
                           (get-group-offsets client topic group))))


(defn to-offset-table [group]
  (fn [[k v]]
    {"Group" group
     "Topic" (.topic k)
     "Partition" (.partition k)
     "Offset" (if (nil? v) "unset" (.offset v))}))

(defn dedupe-map
  ([k1 k2 l]
   (dedupe-map k1 (dedupe-map k2 l)))
  ([k l]
   (flatten
    (for [part (partition-by #(get % k) l)]
      (conj
       (map #(assoc % k "") (rest part))
       (first part))))))

(defn sort-offset-rows [rows]
  (->> (sort-by #(get % "Partition") rows)
       (sort-by #(get % "Topic"))
       (sort-by #(get % "Group"))))

(defn print-groups-offsets [client topics groups]
  (pprint/print-table
   (dedupe-map "Group" "Topic"
               (sort-offset-rows
                (flatten
                 (for [group groups
                       topic topics]
                   (try
                     (map (to-offset-table group)
                          (get-group-offsets client topic group))
                     (catch Exception e
                       {"Group" group "Topic" topic "Partition" -1 "Offset" -1}))))))))

(defn sort-topic-rows [rows]
  (->> (sort-by #(get % "Partition") rows)
       (sort-by #(get % "Topic"))))

(defn to-topic-table [[k1 earliest] [k2 latest]]
  {"Topic" (.topic k1)
   "Partition" (.partition k1)
   "Earliest" (.offset earliest)
   "Latest" (.offset latest)})

(defn print-topics-offsets [client topics]
  (pprint/print-table
   (dedupe-map "Topic"
               (sort-topic-rows
                (flatten
                 (for [topic topics]
                   (try
                     (doall (map to-topic-table
                                 (get-topic-offsets client topic :earliest)
                                 (get-topic-offsets client topic :latest)))
                     (catch Exception e
                       {"Topic" topic "Partition" -1 "Earliest" -1 "Latest" -1}))))))))

(defn reset-group-offsets [client group topic strategy]
  (let [offsets-to-reset-to
        (into {}
              (map (fn [[k v]]
                     [k (OffsetAndMetadata. (.offset v) "")])
                   (get-topic-offsets client topic strategy)))]
    (-> (.alterConsumerGroupOffsets client group offsets-to-reset-to)
        .all
        .get)))

(defn do-group-offset-reset [client topics groups strategy execute]
  (if execute
    (doseq [group groups
            topic topics]
      (try
        (reset-group-offsets client group topic strategy)
        (catch Exception e
          (println "Exception while resetting group offset: " e))))
    (do
      (println (format "Not executing offset reset. New offsets will be: "))
      (doseq [group groups
              topic topics]
        (let [new-offsets (try
                            (map (fn [[k v]] (format "%s - %s: %s"
                                                     (.topic k)
                                                     (.partition k)
                                                     (.offset v)))
                                 (get-topic-offsets client topic strategy))
                            (catch Exception e (list (format "Unknown topic %s" topic))))]
          (println (str/join \newline new-offsets)))))))

(defn option-set [previous key val]
  (assoc previous key
         (if-let [oldval (get previous key)]
           (merge oldval val)
           (hash-set val))))

(def opts
  [["-e" "--execute" "Execute a potentially destructive action."]
   ["-c" "--config FILE" "Use a different config file from the default (\"./kafkaconfig.edn\")" :default "./kafkaconfig.edn"]
   ["-t" "--topic TOPIC" "The topic(s) to act on. Can be provided multiple times." :assoc-fn option-set]
   ["-g" "--group GROUP" "The group(s) to act on. Can be provided multiple times." :assoc-fn option-set]
   ["-s" "--strategy STRATEGY" "Which offset strategy to use. Either \"earliest\" or \"latest\"." :parse-fn keyword :default :earliest]
   ["-h" "--help" "Print help"]])

(defn verify-args [action options]
  (let [required (:required-args (action actions))
        all-present (every? #(get options %) required)
        missing (map name (filter #(nil? (get options %)) required))]
    (when (not all-present)
      (exit (error (format "Missing required argument(s) for action %s: %s" (name action) (str/join ", " missing))) 2))))

(defn execute-action [config action options]
  (verify-args action options)
  (let [kafka-client (client config)]
    (condp = action
      :topic-offsets (print-topics-offsets kafka-client (:topic options))
      :group-offsets (print-groups-offsets kafka-client (:topic options) (:group options))
      :reset-offsets (do-group-offset-reset kafka-client (:topic options) (:group options) (:strategy options) (:execute options)))))

(defn try-read-config [configname]
  (try
    (edn/read-string (slurp configname))
    (catch java.io.FileNotFoundException e
      (exit (error (format "Config file %s not found." configname)) 1))))

(defn run [args]
  (let [{:keys [options errors summary arguments]} (cli/parse-opts *command-line-args* opts)]
    (binding [kafka-config  (try-read-config (:config options))]
      (cond
        errors (exit (error (str/join ", " errors) summary) 1)
        (:help options) (exit (usage summary) 0)
        (< (count arguments) 2) (exit (error "Provide profile and action" summary) 1)
        (nil? (get kafka-config (keyword (first arguments)))) (exit (error "Provide valid profile." summary) 1)
        (nil? (get actions (keyword (second arguments)))) (exit (error "Provide valid action." summary) 1)
        true (execute-action (get kafka-config (keyword (first arguments))) (keyword (second arguments)) options)))))

(defn -main [& args]
  (run *command-line-args*))
