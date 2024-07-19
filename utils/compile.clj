(ns compile
  (:require
   [babashka.fs :as fs]
   [babashka.process :as proc]))

(defn compile-project [& args]
  (compile 'offsets.main))

(defn compile-native [& args]
  (if (fs/exists? "target/offsets.jar")
    (proc/shell "native-image -jar target/offsets.jar --features=clj_easy.graal_build_time.InitClojureClasses --no-fallback -march=native --color=never -o target/offsets")
    (println "Generate classes with clj -M:uberdeps")))
