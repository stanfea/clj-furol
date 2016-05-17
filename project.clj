(defproject furol "0.1.0"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.taoensso/timbre "4.3.1"]
                 [org.clojars.datasio/clj-excel "0.0.1-9780eddfde-17-May-2014"]
                 [org.clojure/data.csv "0.1.3"]
                 [commons-io/commons-io "2.4"]
                 [org.clojure/java.jdbc "0.6.0-alpha1"]
                 [org.clojure/core.match "0.3.0-alpha4"]
                 [org.clojure/core.async "0.2.374"]
                 [org.clojure/tools.cli "0.3.3"]
                 [org.fusesource.hawtjni/hawtjni-runtime "1.11"]
                 [sqljdbc/sqljdbc "4.0"]]
  :main ^:skip-aot furol.core
  :target-path "target/%s"
  :uberjar-name "furol.jar"
  :jvm-opts ["-Djava.library.path=resources/META-INF/windows64"]
  :profiles {:uberjar {:aot :all}})
