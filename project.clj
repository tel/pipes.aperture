(defproject aperture "0.0.1"
  :description "Applying ZMQ broadcasting to pipey computations;
                create portals between your pipelines."
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [pipes/pipes "0.0.1"]
                 [slingshot "0.10.1"]
                 [cascalog/carbonite "1.0.5"]]
  :dev-dependencies [[native-deps "1.0.5"]]
  :native-dependencies [[org.clojars.starry/jzmq-native-deps "2.0.10"]]
  :repl-options [:init nil :caught clj-stacktrace.repl/pst+])