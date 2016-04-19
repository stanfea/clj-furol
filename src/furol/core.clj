(ns furol.core
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.core.match :refer [match]]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.jdbc :refer [execute! create-table-ddl get-connection insert-multi!]]
            [taoensso.timbre :as timbre
             :refer (log  trace  debug  info  warn  error  fatal  report
                          logf tracef debugf infof warnf errorf fatalf reportf
                          spy get-env log-env)]
            [taoensso.timbre.profiling :as profiling
             :refer (pspy pspy* profile defnp p p*)])
  (:import org.apache.commons.io.FilenameUtils
           org.fusesource.hawtjni.runtime.Library)
  (:gen-class))

(def m {:Timestamp 0 :SecurityID 1 :Ticker 2 :Type 3 :Side 4 :Level 5 :Quantity 6 :Price 7 :OrderCount 8 :Flags 9})

(def table-columns [[:id :int "IDENTITY(1,1) PRIMARY KEY"] [:ctimestamp :bigint] [:csecurityid :int] [:cticker :text] [:csellhigh :bigint]
                    [:cselllow :bigint] [:csellopen :bigint] [:csellclose :bigint] [:cbuyhigh :bigint] [:cbuylow :bigint]
                    [:cbuyopen :bigint] [:cbuyclose :bigint] [:chigh :bigint] [:clow :bigint] [:copen :bigint] [:cclose :bigint]])

(defn middle [price1 price2]
  (long (/ (+ price1 price2) 2)))

(defn min-max [coll]
  "calculates min and max in one iteration"
  (->> coll
       rest
       (reduce (fn [[current-min current-max] v]
                 (if (< v current-min)
                   [v current-max]
                   (if (> v current-max)
                     [current-min v]
                     [current-min current-max]))) [(first coll) (first coll)])))

(defn calc [rows]
  (let [prices (map :price rows)
        open (first prices)
        close (last prices)
        [low high] (min-max prices)]
    [high low open close]))

(defn row [buy sell]
  (let [fields [:timestamp :secid :ticker]
        get-fields (fn [rows] (let [row1 (first rows)] (map #(get row1 %) fields)))]
    (match [sell buy]
           [_ nil] (concat (get-fields sell) (calc sell) (repeat 8 0))
           [nil _] (concat (get-fields buy) (repeat 4 0) (calc buy) (repeat 4 0))
           :else (let [timestampsecidticker (get-fields sell)
                       sell (calc sell)
                       buy (calc buy)]
                   (concat timestampsecidticker sell buy (map #(middle %1 %2) sell buy))))))

(defn process [precision reader]
  (let [data (csv/read-csv reader)
        rows (->> data rest (filter #(contains? #{"161" "97"} (nth % (m :Type))))
                  (map (fn [[Timestamp SecurityID Ticker Type Side Level Quantity Price OrderCount Flag]]
                         {:timestamp (Long/parseLong (subs Timestamp 0 precision))
                          :secid (Integer/parseInt SecurityID)
                          :ticker Ticker
                          :type (if (= "97" Type) :sell :buy)
                          :price (Long/parseLong Price)})))
        partitions (partition-by :timestamp rows)]
    (->> partitions
         (mapcat (fn [partition]
                   (->> partition
                        (group-by #(conj [] (get % :ticker) (get % :secid)))
                        vals
                        (map (fn [rows]
                               (let [{buy :buy sell :sell} (group-by :type rows)]
                                 (row buy sell))))))))))

(def required-opts #{:dir :database :table})

(defn missing-required?
  "Returns true if opts is missing any of the required-opts"
  [opts]
  (not-every? opts required-opts))

(def cli-options
  [["-i" "--dir INPUTDIR" "Input directory"
    :validate [#(.exists (io/file %)) "Input dir must exist"]]
   ["-s" "--server-name localhost\\sqlexpress" "database host"
    :default "localhost\\sqlexpress"]
   ["-d" "--database futures" "database name"]
   ["-t" "--table hourly" "database table"]
   ["-u" "--user admin" "sql username"]
   ["-p" "--pass secret123" "sql password"]
   ["-n" "--precision 10" "number of digits to keep in timestamp"
    :parse-fn #(Integer/parseInt %)
    :default 10]
   [nil "--port 1433" "database port"
    :parse-fn #(Integer/parseInt %)
    :default 1433]
   ;;    [nil "--threads nthreads" "number of files to process at a time"
   ;;     :parse-fn #(Integer/parseInt %)
   ;;     :default 1]
   ["-h" "--help"]])

(defn usage [options-summary]
  (->> [""
        "FuRol"
        ""
        "rolls up csv futures files to hourly counts in MS SQL"
        ""
        "Usage: java -jar furol.jar -i c:\\Users\\user\\datadir -d database -t table"
        ""
        "Options:"
        options-summary
        ""]
       (string/join \newline)))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (string/join \newline errors)))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn -main [& args]
  (Library. "jdbc_auth" nil (.getContextClassLoader (Thread/currentThread)))
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)
        {:keys [dir server-name precision database table user pass port]} options]
    (cond
      (:help options) (exit 0 (usage summary))
      (missing-required? options) (exit 0 (usage summary))
      errors (exit 1 (error-msg errors)))
    (let [db  (if (some nil? [user pass])
                {:classname "com.microsoft.jdbc.sqlserver.SQLServerDriver"
                 :subprotocol "sqlserver"
                 :subname (str "//" server-name ":" port ";databasename=" database ";integratedSecurity=true")}
                {:classname "com.microsoft.jdbc.sqlserver.SQLServerDriver"
                 :subprotocol "sqlserver"
                 :subname (str "//" server-name ":" port ";databasename=" database "user=" user "password=" pass)})
          conn (get-connection db)
          query (create-table-ddl (keyword table) table-columns)
          _ (try (execute! db query) (catch Exception e)) ;catch exception if table already exists
          files
          (->> dir
               io/file
               file-seq
               rest
               (map str)
               (filter #(.endsWith % ".csv")))
          nfiles (count files)
          i (atom 0)
          log-progress #(do (swap! i inc) (info (format "progress: %.2f%%" (float (/ (* @i 100) nfiles)))))
          headers (->> table-columns rest (map #(nth % 0)))]
      (pr-str headers)
      (->> files
           (map (fn [fpath]
                  (info "processing:" fpath)
                  (let [basename (FilenameUtils/getBaseName fpath)]
                    (try
                      (with-open [in (io/reader fpath)]
                        (->> (doseq [batch (partition-all 1000 (process precision in))]
                               (insert-multi! db table headers batch))
                             doall)
                        (log-progress))
                      (catch Throwable t (error t))))))
           doall))))

