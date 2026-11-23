"""
Task 1. Expedia Hotel Search Analytics
"""

import os
import socket
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.sql.functions import (col, to_timestamp,to_date, month, dayofweek, hour,count, countDistinct,sum as spark_sum, when, avg, lit,round as spark_round, row_number)


#helper function for time stamp in the screenshots
def print_section(title,value=None):
    print("\n" + "-" *80)
    print(title)
    if value is not None:
        print(value)
    print("Terminal timestamp:",datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

if __name__ == "__main__":

    # Starting the Spark application
    spark = SparkSession \
        .builder \
        .appName("Task1_Expedia_Search_Analytics") \
        .getOrCreate()

    #Reduce the amount of Spark system output in terminal
    spark.sparkContext.setLogLevel("ERROR")

    print("Spark version:", spark.version)
    print("Driver running on:", socket.gethostname())

    # Bucket and file paths used 
    # The sample file is stored in my personal bucket for testing
    # On the cluster these come from the environment; for a local run set
    # HOTELS_PATH and OUTPUT_PATH instead (see README)
    bucket_name = os.environ.get("BUCKET_NAME", "")
    data_bucket = os.environ.get("DATA_REPOSITORY_BUCKET", "")

    # Full Expedia dataset from the shared module bucket
    input_path = os.environ.get("HOTELS_PATH") or "s3a://" + data_bucket + "/Expedia/hotels.csv"

    # Separate final output folder so sample results are not overwritten
    output_path = os.environ.get("OUTPUT_PATH") or "s3a://" + bucket_name + "/task1/final_output"

    print("Input path:", input_path)
    print("Output path:",output_path)

   # Reading Expedia hotel search data
    # inferSchema is used here so Spark can detect numeric and timestamp columns automatically
    hotels_df = (spark.read.option("header","true").option("inferSchema", "true").csv(input_path))

    # Q1. Load and sanity check

    # Inspect schema before applying transformations
     # Checking column names and data types before doing any analysis
    print_section("Q1. Dataset columns")
    print(hotels_df.columns)

    print_section("Q1. Inferred schema")
    hotels_df.printSchema() #Counting number of displayed hotel results and number of unique searches
    impression_count = hotels_df.count()
    search_count = hotels_df.select("srch_id").distinct().count()

    print_section("Q1.Basic dataset counts")
    print("Number of rows / hotel impressions:", impression_count)
    print("Number of distinct search IDs:", search_count)

    #same DataFrame is used several times therefore it is cached after the first checks
    hotels_df = hotels_df.cache()
    hotels_df.count()

    # confirming the file is  loaded correctly
    print("Q1.Preview of the loaded data")
    hotels_df.show(10, truncate=False)

    
    # Q2. Time feature engineering

    # Turning original search time into a Spark timestamp and then extract useful time fiel
    hotels_df = hotels_df.withColumn("timestamp",to_timestamp(col("date_time")))

    hotels_df =hotels_df.withColumn("date",to_date("timestamp")) \
                         .withColumn("month", month("timestamp")) \
                         .withColumn("dow", dayofweek("timestamp")) \
                         .withColumn("hour",hour("timestamp"))

    #check that the new columns have been added correctly
    print_section("Q2 Time columns added to the data")
    time_cols= ["srch_id","date_time","timestamp","date","month","dow","hour"]

    hotels_df.select(time_cols).show(10, truncate=False)

    
    # Q3. Search activity over time

    # Building a daily summary of search activity and user responses
    daily_activity= hotels_df.groupBy("date").agg(
        count("*").alias("total_impressions"),
        countDistinct("srch_id").alias("total_searches"),
        spark_sum(col("click_bool").cast("int")).alias("total_clicks"),
        spark_sum(col("booking_bool").cast("int")).alias("total_bookings"))

    # Add the two performance ratios requested in the task description
    daily_activity =(
        daily_activity
            .withColumn("ctr",spark_round(col("total_clicks") / col("total_impressions"),4))
            .withColumn("conversion_rate",spark_round(col("total_bookings") /col("total_impressions"),4))
            .orderBy("date")
    )

    print_section("Q3.Daily search activity summary")
    daily_activity.show(10, truncate=False)

    # Export this table so it can be downloaded and plotted outside the Spark job
    daily_activity.coalesce(1) \
        .write\
        .mode("overwrite") \
        .option("header","true") \
        .csv(output_path +"/q3_daily_activity")

    # Q4. Time-of-day behaviour

    # Summarise activity by search hour and whether the search includes a Saturday night stay
    hourly_activity =hotels_df.groupBy( "hour","srch_saturday_night_bool").agg(count("*").alias("total_impressions"),
                                                                               spark_sum(col("click_bool").cast("int")).alias("total_clicks")
                                                                              )

    hourly_activity = (hourly_activity.withColumn("ctr",spark_round(col("total_clicks")/col("total_impressions"), 4)).orderBy("hour","srch_saturday_night_bool"))

    ## printing 2 tables as 1 tables does not fir in screen shot
    print_section("Q4. Hourly activity, hours 0 to 11")
    hourly_activity.filter(col("hour") <= 11).show(24, truncate=False)

    print_section("Q4. Hourly activity, hours 12 to 23")
    hourly_activity.filter(col("hour") >= 12).show(24, truncate=False)
    
    #Saving hourly activity output for plotting outside the Spark 
    hourly_activity.coalesce(1) \
        .write \
        .mode("overwrite")\
        .option("header", "true") \
        .csv(output_path + "/q4_hourly_activity")

    #Q5.Position Bias Analysis
   # Group the original hotel star ratings into the four bands requested in the task
    hotels_df = hotels_df.withColumn(
        "star_band",
        when(col("prop_starrating").cast("int").between(0, 2), "0-2")
        .when(col("prop_starrating").cast("int") == 3,"3")
        .when(col("prop_starrating").cast("int") == 4, "4")
        .when(col("prop_starrating").cast("int") == 5,"5")
        .otherwise("unknown"))

    # Count impressions, clicks and bookings for each position/star-band combination
    position_bias =(
        hotels_df.where(col("star_band")!="unknown").groupBy(col("position").cast("int").alias("position"),"star_band").agg(
            count("*").alias("total_impressions"),
            spark_sum(col("click_bool").cast("int")).alias("total_clicks"),
            spark_sum(col("booking_bool").cast("int")).alias("total_bookings")
        )
    )

    # Ading  CTR and conversion rate and then sorting by ranking position
    position_bias = (
        position_bias
        .withColumn("ctr",spark_round(col("total_clicks") /col("total_impressions"),4))
        .withColumn("conversion_rate",spark_round(col("total_bookings") / col("total_impressions"), 4))
        .orderBy("position", "star_band")
    )

    print_section("Q5. Position bias for positions 1 to 5")
    position_bias.filter(col("position") <= 5).show(20, truncate=False)
    
    #Saving position bias output for plotting outside Spark
    position_bias.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header","true")\
        .csv(output_path +"/q5_position_bias")

    
    # Q6. Monthly destination popularity and performance

    # First summarise each destination within each month
    monthly_destination =hotels_df.groupBy("month",col("srch_destination_id").cast("int").alias("srch_destination_id")).agg(
        count("*").alias("total_impressions"),
        spark_sum(col("booking_bool").cast("int")).alias("total_bookings"))

    monthly_destination = monthly_destination.withColumn("conversion_rate", spark_round(col("total_bookings") / col("total_impressions"),4))

    #Window specification for ranking destination by number of bookings
    bookings_window =Window.partitionBy("month").orderBy(col("total_bookings").desc(),col("total_impressions").desc())

    top10_destinations_by_bookings= (
        monthly_destination.withColumn("rank",row_number().over(bookings_window))
            .where(col("rank")<=10)
            .orderBy("month", "rank"))

    # Windowspecification for ranking destinations by conversion rate
    conversion_window = Window.partitionBy("month").orderBy(col("conversion_rate").desc(),
                                                            col("total_bookings").desc(),
                                                            col("total_impressions").desc())

    top10_destinations_by_conversion=(
        monthly_destination.withColumn("rank",row_number().over(conversion_window))
            .where(col("rank")<= 10)
            .orderBy("month","rank"))

    selected_month= 5

    print_section("Q6.Selected Month:", selected_month)

    print_section("Q6. Top 10 destinations by bookings for May")
    top10_destinations_by_bookings \
        .filter(col("month") == selected_month) \
        .show(10, truncate=False)

    print_section("Q6. Top 10 destinations by conversion rate for May")
    top10_destinations_by_conversion\
        .filter(col("month") ==selected_month)\
        .show(10, truncate=False)

    # Saving full monthly destination outputs for plotting and reporting
    top10_destinations_by_bookings.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(output_path + "/q6_top10_destinations_by_bookings")

    top10_destinations_by_conversion.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header","true") \
        .csv(output_path + "/q6_top10_destinations_by_conversion")


    # Q7. Detecting unusual search activity

    # Use the daily summary from Q3 to calculate the average number of searches per day
    mean_searches = (daily_activity.select(avg("total_searches").alias("mean_searches")).first()["mean_searches"])

    print_section("Q7.Mean daily search count:",round(mean_searches,4))

    # Keep only the dates where search activity is higher than the overall daily average
    unusual_activity = daily_activity.withColumn("mean_daily_searches",spark_round(lit(mean_searches),4))

    unusual_activity = (unusual_activity.where(col("total_searches") > col("mean_daily_searches")).orderBy("date"))

    print_section("Q7.Dates where search activity is above the daily mean")
    unusual_activity.show(20,truncate=False)

    # Save unusual activity output for plotting
    unusual_activity.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header","true") \
        .csv(output_path+ "/q7_unusual_activity")

    spark.stop()
