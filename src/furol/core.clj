(ns furol.core
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.core.async :refer [close! chan <! >! >!! <!! go go-loop]]
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
           org.fusesource.hawtjni.runtime.Library
           java.util.concurrent.Executors)
  (:gen-class))


(def m {:Timestamp 0 :SecurityID 1 :Ticker 2 :Type 3 :Side 4 :Level 5 :Quantity 6 :Price 7 :OrderCount 8 :Flags 9})

(def table-columns [:ctimestamp :csecurityid :cticker :csellhigh :cselllow :csellopen :csellclose :cbuyhigh :cbuylow :cbuyopen :cbuyclose :chigh:clow :copen :cclose])

(defn table-spec [table] (format "CREATE TABLE %s (id int IDENTITY(1,1) PRIMARY KEY, ctimestamp bigint, csecurityid int, cticker VARCHAR(20), csellhigh bigint,
                                 cselllow bigint, csellopen bigint, csellclose bigint, cbuyhigh bigint, cbuylow bigint,
                                 cbuyopen bigint, cbuyclose bigint, chigh bigint, clow bigint, copen bigint, cclose bigint,
                                 CONSTRAINT uc_log UNIQUE(ctimestamp,cticker,csecurityid))" table))

(def weekdays {1 :monday 2 :tuesday 3 :wednesday 4 :thursday 5 :friday 6 :saturday 7 :sunday})


(defn trading-hours? [{:keys [timestamp] :as record}]
  (let [ct-datetime (t/to-time-zone (f/parse (f/formatter "yyyyMMddHH") timestamp) (t/time-zone-for-id "America/Chicago"))
        weekday (->> ct-datetime .dayOfWeek .get weekdays)
        hour (.getHourOfDay ct-datetime)]
    (cond
      (= :saturday weekday) false
      (and (= :friday weekday) (>= hour 16)) false
      (and (= :sunday weekday) (< hour 17)) false
      (= 16 hour) false
      :else true)))

(defn middle [price1 price2]
  (long (/ (+ price1 price2) 2)))

(defn min-max [coll]
  "calculates min and max in one iteration"
  (->> coll
       rest
       (reduce (fn [[current-min current-max] v]
                 (cond
                   (< v current-min) [v current-max]
                   (> v current-max) [current-min v]
                   :else [current-min current-max])) [(first coll) (first coll)])))

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


(defn add-date-time [{:keys [ctimestamp] :as record}]
  (->> ctimestamp
       str
       (f/parse (f/formatter "yyyyMMddHH"))


(defn process [precision reader]
  (let [data (csv/read-csv reader)
        rows (->> data rest (filter #(contains? #{"161" "97"} (nth % (m :Type))))
                  (map (fn [[Timestamp SecurityID Ticker Type Side Level Quantity Price OrderCount Flag]]
                         {:timestamp (subs Timestamp 0 precision)
                          :secid (Integer/parseInt SecurityID)
                          :ticker Ticker
                          :type (if (= "97" Type) :sell :buy)
                          :price (Long/parseLong Price)}))
                  (filter trading-hours?)
                  (map (partial update-in :timestamp #(Long/parseLong %))))
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

   ["-d" "--database futures" "database name"]
   ["-t" "--table hourly" "database table"]
   [nil "--server-name localhost\\sqlexpress" "database host"
    :default "localhost\\sqlexpress"]
   [nil "--user admin" "sql username"]
   [nil "--pass secret123" "sql password"]
   [nil "--precision 10" "number of digits to keep in timestamp"
    :parse-fn #(Integer/parseInt %)
    :default 10]
   [nil "--processed-dir inputdir/processed" "Move processed files to this directory"]
   [nil "--batchsize 10000" "how many rows to insert at a time to sql server"
    :parse-fn #(Integer/parseInt %)
    :default 10000]
   [nil "--port 1433" "database port"
    :parse-fn #(Integer/parseInt %)
    :default 1433]
   [nil "--threads nthreads" "number of files to process at a time"
    :parse-fn #(Integer/parseInt %)
    :default 3]
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


(defn get-files [dir extension & {:keys [recursive?] :or {:recursive? false}}]
  (let [listfn (if recursive? file-seq #(.listFiles %))]
    (->> dir
         io/file
         listfn
         seq
         (map str)
         (filter #(= extension (FilenameUtils/getExtension %))))))

(defn try-times*
  "Executes thunk. If an exception is thrown, will retry. At most n retries
  are done. If still some exception is thrown it is bubbled upwards in
  the call chain."
  [n thunk]
  (loop [n n
         sleep 100]
    (if-let [result (try
                      [(thunk)]
                      (catch Exception e
                        (warn e)
                        (if (not (= -1 (.indexOf (.getMessage e) "PRIMARY")))
                          (when (zero? n)
                            (throw e))
                          (fn [x] 0))))]
      (result 0)
      (do
        (warn "sleeping for" sleep "ms before retrying...")
        (Thread/sleep sleep)
        (recur (dec n) (+ sleep 100))))))

(defmacro try-times
  "Executes body. If an exception is thrown, will retry. At most n retries
  are done. If still some exception is thrown it is bubbled upwards in
  the call chain."
  [n & body]
  `(try-times* ~n (fn [] ~@body)))

(defn make-db-spec [server-name port database user pass]
  (if (some nil? [user pass])
    {:classname "com.microsoft.jdbc.sqlserver.SQLServerDriver"
     :subprotocol "sqlserver"
     :subname (str "//" server-name ":" port ";databasename=" database ";integratedSecurity=true")}
    {:classname "com.microsoft.jdbc.sqlserver.SQLServerDriver"
     :subprotocol "sqlserver"
     :subname (str "//" server-name ":" port ";databasename=" database "user=" user "password=" pass)}))


(defn -main [& args]
  (try
    (.load (Library. "sqljdbc_auth"))
    (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)
          {:keys [dir threads server-name processed-dir precision batchsize database table user pass port]} options]
      (cond
        (:help options) (exit 0 (usage summary))
        (missing-required? options) (exit 0 (usage summary))
        errors (exit 1 (error-msg errors)))
      (let [processed-dir (or processed-dir (str (io/file dir "processed")))
            _ (.mkdir (io/file processed-dir))
            sql-queue (chan)
            db (make-db-spec server-name port database user pass)
            conn (get-connection db)
            query (try (execute! db (table-spec table)) (catch Throwable e (warn "Table" table "exists already")))
            files (get-files dir "csv")
            nfiles (count files)
            headers (->> table-columns rest (map #(nth % 0)))
            pool (Executors/newFixedThreadPool threads)
            i (atom 0)]
        (go-loop [[ack-chan batch] (<! sql-queue)]
                 (when (not (nil? batch))
                   (try-times 100 (insert-multi! db table headers batch))
                   (close! ack-chan)
                   (recur (<! sql-queue))))
        (->> files
             (map (fn [fpath]
                    (fn []
                      (locking *out*
                        (info "processing:" fpath))
                      (with-open [in (io/reader fpath)]
                        (let [filename (FilenameUtils/getName fpath)]
                          (->>
                            in
                            (process precision)
                            (partition-all batchsize)
                            (map (fn [batch]
                                   (let [ack-chan (chan)
                                         batch (doall batch)]
                                     (go (>! sql-queue [ack-chan batch]))
                                     ack-chan)))
                            (map #(<!! %))
                            doall)))
                      (if (not (.renameTo (io/file fpath) (io/file processed-dir (FilenameUtils/getName fpath))))
                        (throw (Exception. (format "Couldn't move %s to %s" fpath  (str (io/file processed-dir (FilenameUtils/getName fpath)))))))
                      (locking *out*
                        (swap! i inc)
                        (info (format "progress: %.2f%%" (float (/ (* @i 100) nfiles))))))))
             (.invokeAll pool)
             (map #(.get %))
             doall)
        (close! sql-queue)
        (.shutdown pool)))
    (catch Throwable t (error t) (throw t))))

