"""
Task 4: Structured Streaming with Smoke Detection IoT Data
"""

import os
import socket
import pytz
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import (split, col, size, from_unixtime, to_timestamp,window, count, avg, when, sum as spark_sum, trim, expr)
import sys

# helper function for timestamped screenshots
def print_section(title, value=None):
    print("\n" + "-" * 90)
    print(title)
    if value is not None:
        print(value)
    uk_time = datetime.now(pytz.timezone("Europe/London"))
    print("Terminal timestamp:", uk_time.strftime("%Y-%m-%d %H:%M:%S"))
    print("-" * 90)


if __name__ == "__main__":

    # Starting the Spark application
    spark = SparkSession\
        .builder\
        .appName("Task4_Smoke_Detection_IoT_Streaming")\
        .getOrCreate()


    #Keeping the terminal output focused on my own results
    spark.sparkContext.setLogLevel("ERROR")

    #using UTC because the dataset stores event times as Unix timestamps
    spark.conf.set("spark.sql.session.timeZone","UTC")

    print("Spark version:",spark.version)
    print("Driver running on:",socket.gethostname())

    #my personal bucket
    # On the cluster this comes from the environment; for a local run set CHECKPOINT_DIR (see README)
    bucket_name = os.environ.get("BUCKET_NAME", "")
    
    #the host and port are provided by the streaming server environment.
    smoke_host = os.getenv("STREAMING_SERVER_SMOKE","smoke-detection.stream-emulator.svc.cluster.local")
    smoke_port = int(os.getenv("STREAMING_SERVER_SMOKE_PORT","5551"))

    
    print("Connecting to IoT stream at:",smoke_host, smoke_port)

    #Reading smoke detection IoT stream 
    logs_df = (spark.readStream.format("socket")
        .option("host", smoke_host)
        .option("port", smoke_port)
        .option("includeTimestamp", "true")
        .load())

    
    # Each incoming row is one CSV line, stream includes a header row, so I remove it before casting.
    parsed_df = (logs_df
        #Spliting each CSV line into separate fields
        .withColumn("data",split(col("value"),","))
        
        #Keeping only rows that look like real data rows
        # This removes the headerline before casting values to numeric types 
        .filter(size(col("data")) >= 16)
        .filter(trim(col("data").getItem(1)).rlike(r"^[0-9]+$"))

        #Converting the Unix UTC timestamp into Spark TimestampType
        .withColumn("event_time",to_timestamp(from_unixtime(trim(col("data").getItem(1)).cast("long"))))

        #extracting and cast the sensor fields
        .withColumn("temperature",trim(col("data").getItem(2)).cast("double"))
        .withColumn("humidity",trim(col("data").getItem(3)).cast("double"))
        .withColumn("tvoc",trim(col("data").getItem(4)).cast("double"))
        .withColumn("eco2",trim(col("data").getItem(5)).cast("double"))
        .withColumn("pm25",trim(col("data").getItem(10)).cast("double"))
        .withColumn("fire_alarm",trim(col("data").getItem(15)).cast("integer"))

        #Selecting only the cleaned columns
        .select("event_time","temperature","humidity","tvoc","eco2","pm25","fire_alarm")

        #Removing any rows where the timestamp couldn't be converted properly
        .filter(col("event_time").isNotNull())
    )

    
    # Q1. Streaming Ingestion and Initial Parsing

    print_section("Q1. Parsed streaming schema")
    parsed_df.printSchema()

    #Printing the parsed stream to the console in append mode
    query1 =(parsed_df.writeStream.outputMode("append").format("console").option("truncate", "false")
        .queryName("Q1_Parsed_Smoke_Data").start())

    #time for the query to run before it stops
    query1.awaitTermination(60)
    query1.stop()


    # Q2. Watermark

    print_section("Q2. Starting watermark query")

    #applying 5-second event-time watermark
    watermarked_df=parsed_df.withWatermark("event_time","5 seconds")

    # Counting readings in 10-second event-time windows
    window_counts_df=(watermarked_df.groupBy(window(col("event_time"),"10 seconds")).agg(count("*").alias("reading_count")))

    #writing completed watermarked window counts to the console
    #Append mode only outputs a window after Spark considers it complete
    query2=(window_counts_df.writeStream.outputMode("append").format("console").option("truncate", "false")
        .queryName("Q2_Watermarked_Window_Counts").start())
    
    query2.awaitTermination(120)
    query2.stop()

  

    # Q3a. Sliding Window Low-Humidity Count

    print_section("Q3a. Starting sliding window low-humidity query")

    # Setting required sliding window parameters:each window covers 60 seconds and, new window starts every 30 seconds.
    windowDuration = "60 seconds"
    slideDuration = "30 seconds"

    #keeping reading only where humidity is below 50
    low_humidity_df=parsed_df.filter(col("humidity") < 50)


    # Counting number of low-humidity readings in each sliding time window: hence the slide is shorter than the window, windows overlap
    low_humidity_counts_df = (low_humidity_df.groupBy(window(col("event_time"), windowDuration, slideDuration))
        .agg(count("*").alias("low_humidity_count")))

    #query is in update mode and update mode prints windows when their counts change only
    query3a = (low_humidity_counts_df.writeStream.outputMode("update").format("console").option("truncate", "false")
        .queryName("Q3a_Low_Humidity_Counts").start())
    
    query3a.awaitTermination(120)
    query3a.stop()

    
    # Q3b. Stateful Alarm-State Counting

    print_section("Q3b. Starting stateful alarm-state count query")

    #keeping a running count of readings grouped by fire alarm state, which is stateful because Spark has to remember the counts across micro-batches
    alarm_counts_df = (parsed_df.groupBy("fire_alarm")
        .agg(count("*").alias("alarm_count"))
        .orderBy(col("alarm_count").desc(),col("fire_alarm").asc()))


    #query is in complete mode it prints the full current grouped state each batch
    query3b = (alarm_counts_df.writeStream.outputMode("complete").format("console").option("truncate", "false")
        .queryName("Q3b_Stateful_Alarm_Counts").start())

    
    query3b.awaitTermination(150)
    query3b.stop()

    
    #Q4. Aggregation and Triggered High-Risk Activity

    # Q4a. Aggregation by Alarm State

    print_section("Q4a. Starting aggregation by alarm state query")

    #grouping stream by fire alarm state and calculating the running averages for temperature and PM2.5
    alarm_average_df = (parsed_df.groupBy("fire_alarm")
        .agg(avg("temperature").alias("avg_temp"),avg("pm25").alias("avg_pm25"))
        .orderBy("fire_alarm"))

    # Query is in compelete mode, it prints the full current averages for each alarm state at every batch
    query4a = (alarm_average_df.writeStream.outputMode("complete").format("console")
        .option("truncate", "false").queryName("Q4a_Alarm_State_Averages").start())
    
    query4a.awaitTermination(120)
    query4a.stop()
    
    
    # Q4b. Triggered Aggregation of Risk Categories
    
    print_section("Q4b. Starting triggered high-risk aggregation query")

    # Creating high-risk flag using the condition:high_risk=1 when eCO2 is at least 415 and humidity is at least 50
    risk_df = (parsed_df.withColumn("high_risk",when((col("eco2") >= 415) & (col("humidity") >= 50), 1).otherwise(0)))

    #counting high-risk and non-high-risk
    risk_counts_df = (risk_df.groupBy("high_risk")
        .agg(count("*").alias("risk_count"))
        .orderBy("high_risk"))

    #starting the query with a fixed 15-second processing-time trigger and complete mode to print full current count for each cat
    query4b = (risk_counts_df.writeStream.outputMode("complete").format("console")
        .option("truncate", "false").trigger(processingTime="15 seconds")
        .queryName("Q4b_High_Risk_Counts").start())

    
    query4b.awaitTermination(120)
    query4b.stop()


   # Q5. Window-Level Threshold Agreement
    print_section("Q5. Starting window-level threshold agreement query")

    # Setting the thresholds
    T=20 #TVOC threshold
    K=1 #minimum counts
    M=1 #minimum counts

    #memory table is used so I can query the current window labels and calculate the confusion matrix in Python.
    q5_table_name="q5_window_labels_live"


    #building the window-level labels
    # adding a watermark on event_time so Spark can manage late data and clean up old window state during the window aggregation
    q5_df=(parsed_df.withWatermark("event_time","2 minutes")

        # Creating a row-level TVOC alert flag:1 when TVOC is at least threshold T, otherwise 0
        .withColumn("tvoc_alert", when(col("tvoc") >= T, 1).otherwise(0))

        #Creating row-level risk flag using the same high-risk rule from Q4b
        .withColumn("risk_alert", when((col("eco2") >= 415) & (col("humidity") >= 50), 1).otherwise(0))

        #grouping readings into 60-second windows that slide every 30 seconds
        .groupBy(window(col("event_time"), "60 seconds", "30 seconds"))

        #count how many TVOC alerts and high-risk readings occur in each window
        .agg(spark_sum("tvoc_alert").alias("tvoc_alert_count"),
             spark_sum("risk_alert").alias("risk_count"))

        #binary TVOC alert label,if at least K readings in the window have TVOC >= 20, window is marked as 1
        .withColumn("window_tvoc_alert", when(col("tvoc_alert_count") >= K,1).otherwise(0))

        #binary risk label:where at least M readings in the window are high-risk the window is marked as 1
        .withColumn("window_risk",when(col("risk_count") >= M,1).otherwise(0))

        #selecting cols needed
        .select("window", "tvoc_alert_count", "risk_count", "window_tvoc_alert", "window_risk"))


    #Writing the query in memory table to be ablee to query the results with Spark SQL
    query5 = (q5_df.writeStream.outputMode("complete").format("memory")
        .queryName(q5_table_name).trigger(processingTime="15 seconds").start())
    
    try:

        # Printing enough batches for screenshots of the labels and confusion matrix
        for i in range(8):
            time.sleep(16)
    
            print("\n" + "-" * 90)
            print_section("Q5 Batch:",i + 1)
            print("-" * 90)

            #Reading latest window labels from memory table,SQL query
            window_labels_df= spark.sql(f"SELECT * FROM {q5_table_name}")
    
            print("Window labels:")
            window_labels_df.show(50, truncate=False)

            #Building the 2x2 confusion matrix
            confusion_df=(window_labels_df.groupBy("window_tvoc_alert","window_risk").count().orderBy("window_tvoc_alert","window_risk"))
    
            print("Confusion matrix:")
            confusion_df.show(truncate=False)

            #collecting the small confusion matrix so precision and recall can be computed
            rows = confusion_df.collect()
            #Starting all confusion-matrix counts at zero
            tp = fp = fn = tn = 0

            # window_tvoc_alert is the prediction and window_risk is treated as baseline or ground truth
            for row in rows:
                pred=row["window_tvoc_alert"]
                real=row["window_risk"]
                total=row["count"]

                #assigning each grouped count to right confusion matrix category
                if pred == 1 and real == 1:
                    tp += total #True positive
                elif pred == 1 and real == 0:
                    fp += total#false positive
                elif pred == 0 and real == 1:
                    fn += total#false negative
                elif pred == 0 and real == 0:
                    tn += total#true negative

            #Computing precision and recall for the window-level TVOC alert and avoiding division by zero if a cat is missing
            precision = tp / (tp + fp) if (tp + fp) > 0 else 0
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    
            print("TP:", tp,"FP:", fp,"FN:", fn,"TN:", tn)
            print("Precision:",precision)
            print("Recall:",recall)
    
    finally:
        if query5.isActive: #Stoping the streaming query cleanly
            query5.stop()


    
    # Q6. Fault Tolerance and Recovery

    print_section("Q6. Fault Tolerance and Recovery")

    #using fewer shuffle partitions to make stateful aggregation runs faster
    spark.conf.set("spark.sql.shuffle.partitions","2")


    #Storing checkpoint in my own S3 bucket, so Spark uses this path to save progress and state for recovery
    checkpoint_dir = os.environ.get("CHECKPOINT_DIR") or f"s3a://{bucket_name}/task4/checkpoints/q6_stateful_alarm_results"
    
    print("Q6 checkpoint path:", checkpoint_dir)

    
    #Reusing Q3b stateful alarm-count aggregation,Spark keeps the running count for each fire_alarm value across micro-batches
    alarm_counts_df = (parsed_df.groupBy("fire_alarm").agg(count("*").alias("alarm_count")))
    

    # First run
    print_section("Q6. First run before stopping")


    #Starting first version of the query with checkpointing working,complete mode prints full current alarm-count state each batch
    query6_first=(alarm_counts_df.writeStream.outputMode("complete").format("console").option("truncate","false")
        .option("checkpointLocation", checkpoint_dir).trigger(processingTime="20 seconds").start())
    
    query6_first.awaitTermination(90)

    #stopping the query to simulate interruption failure
    query6_first.stop()

    
    # Restart from same checkpoint
    print_section("Q6. Restarting from the same checkpoint")

    #restarting same stateful query using same checkpoint path, if working count should continue from the previous state
    query6_restart=(alarm_counts_df.writeStream.outputMode("complete").format("console").option("truncate","false")
        .option("checkpointLocation",checkpoint_dir).trigger(processingTime="20 seconds").start())
    
    try:
        #waiting briefly to capture restored state after restart,
        #as the socket source is not fully replayable,i stop it soon after recovery output is out
        time.sleep(10)
    finally:
        #Stopping restarted query cleanly
        if query6_restart.isActive:
            query6_restart.stop()

    
    #forcing spark to stop
    for q in spark.streams.active:
        try:
            q.stop()
        except Exception:
            pass


    spark.stop()

        
    