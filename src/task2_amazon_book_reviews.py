"""
Task 2. Analysis of Book Reviews Data
"""
import os
import socket
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (col,count,countDistinct, from_unixtime,to_timestamp, date_format,year,month, when, sum as spark_sum, round as spark_round,lit, avg,lag, min as spark_min, max as spark_max)
import pytz #handling time zone

# helper function for timestamped screenshots
def print_section(title, value=None):
    print(title)
    if value is not None:
        print(value)
    uk_time =datetime.now(pytz.timezone('Europe/London'))
    print("Terminal timestamp:",uk_time.strftime("%Y-%m-%d %H:%M:%S"))
    print("-" *80)

if __name__ == "__main__":

    #starting the Spark application
    spark = SparkSession \
        .builder \
        .appName("Task2_Amazon_Book_Reviews_Analysis") \
        .getOrCreate()
    #keeping terminal output focused on my own results rather than Spark system logs
    spark.sparkContext.setLogLevel("ERROR")

    print("Spark version:", spark.version)
    print("Driver running on:", socket.gethostname())

    #Bucket paths
    # On the cluster these come from the environment; for a local run set
    # RATINGS_PATH, BOOKS_PATH and OUTPUT_PATH instead (see README)
    bucket_name = os.environ.get("BUCKET_NAME", "")
    data_bucket = os.environ.get("DATA_REPOSITORY_BUCKET", "")

    #input paths from module bucket
    ratings_path = os.environ.get("RATINGS_PATH") or "s3a://" + data_bucket + "/AmazonBooks/Books_rating.csv"
    books_path = os.environ.get("BOOKS_PATH") or "s3a://" + data_bucket + "/AmazonBooks/books_data.csv"

    #output path
    output_path = os.environ.get("OUTPUT_PATH") or "s3a://" + bucket_name + "/task2/final_output"

    print("Ratings input path:", ratings_path)
    print("Books input path:", books_path)
    print("Output path:", output_path)

    #Q1.Load and Data Cleaning

    #Loading ratings data
    ratings_df =(spark.read.option("header","true").option("inferSchema","true").csv(ratings_path))

    # Loading books metadata
    books_df = (spark.read.option("header","true").option("inferSchema","true").csv(books_path))

    #showing schemas before cleaning
    print_section("Q1.Ratings DataFrame schema")
    ratings_df.printSchema()

    print_section("Q1.Books DataFrame schema")
    books_df.printSchema()
    
    #checking invalid review time values before cleaning
    invalid_reviews_df =ratings_df.filter(col("review_time").isNull() |(col("review_time").cast("long").isNull())|
                                          (col("review_time").cast("long") <= 0))
    #counting invalid resullts
    invalid_count = invalid_reviews_df.count()
    print_section("Q1. Invalid review_time values found")
    print("Total invalid rows:",invalid_count)

    # validation
    if invalid_count > 0:
        print("\nSample of the invalid rows:")
        invalid_reviews_df.select("id","user_id","review_score","review_time").show(10,truncate=False)

    #looking at missing categories before cleaning
    missing_categories_count =books_df.filter(col("categories").isNull()).count()
    
    print("Rows with null categories:",missing_categories_count)

    #cleaning
    #replacing missing categories with Other
    books_df = books_df.fillna({"categories":"Other"})

    #droping nvalid review_time rows
    #review_time is Unix timestamp therefore null, non-numeric, zero, and negative values are removed
    ratings_df = ratings_df.filter(col("review_time").isNotNull() &col("review_time").cast("long").isNotNull() &
                                   (col("review_time").cast("long") > 0))

    #Validation after cleaning
    remaining_invalid_count = ratings_df.filter(col("review_time").isNull() |col("review_time").cast("long").isNull() |
                                                (col("review_time").cast("long") <= 0)).count()

    remaining_missing_categories = books_df.filter(col("categories").isNull()).count()

    print("Remaining invalid review_time rows:",remaining_invalid_count)
    print("Remaining null categories:",remaining_missing_categories)

    #Computing required counts after cleaning
    total_reviews= ratings_df.count()
    distinct_users =ratings_df.select("user_id").distinct().count()
    distinct_books =ratings_df.select("id").distinct().count()

    print_section("Q1. Computed counts after cleaning")
    print("Total number of reviews:",total_reviews)
    print("Total number of distinct users:",distinct_users)
    print("Total number of distinct books:",distinct_books)

    #Q2.Time Conversion and Sorting
    
    #review_time is stored as a Unix timestamp
    # So i converted to Spark timestamp format
    ratings_df = ratings_df.withColumn("review_timestamp",to_timestamp(from_unixtime(col("review_time").cast("long"))))

    #making clean date column in YYYY-MM-DD format
    ratings_df =ratings_df.withColumn("review_date",date_format(col("review_timestamp"), "yyyy-MM-dd"))

    #Extracting year and month from converted timestamp.
    ratings_df = (
        ratings_df
        .withColumn("review_year", year(col("review_timestamp")))
        .withColumn("review_month", month(col("review_timestamp")))
    )

    # Categorising the  review months into Early Year and Late Year
    ratings_df=(
        ratings_df
        .withColumn("half_year", when(col("review_month").between(1,6),"Early Year").otherwise("Late Year"))
    )


    #Sorting cleaned ratings data 
    ratings_df= ratings_df.orderBy("review_date")

    print_section("Q2. Sorted rows after converting review_time")
    ratings_df.select("id","user_id","review_score","review_time","review_timestamp","review_date").show(10, truncate=False)

    print_section("Q2.Date-related columns")
    ratings_df.select("id","review_time","review_timestamp","review_date",
                      "review_year","review_month","half_year").show(10, truncate=False)

    #Saving output 
    ratings_df.select("id","user_id","review_score","review_time", "review_timestamp","review_date",
                      "review_year","review_month","half_year")\
        .coalesce(1) \
        .write\
        .mode("overwrite") \
        .option("header","true")\
        .csv(output_path +"/q2_time_converted_reviews")
    
    #checking the range of the dataset to find out the exact time span as the project description states it starts from 1996 but there is data from 1995
    print_section("Q2.Date range after converting review_time")
    
    ratings_df.select(spark_min("review_date").alias("earliest_review_date"),
                      spark_max("review_date").alias("latest_review_date")).show(truncate=False)
    
    #Q3. Rating Distribution and User Behaviour
    
    # Creating rating bands: Low ratings 0 to 2,Medium ratings 3 to 4,and High ratings 5.
    ratings_df = ratings_df.withColumn(
        "rating_band",
        when(col("review_score").between(0, 2), "Low")
        .when(col("review_score").between(3, 4), "Medium")
        .when(col("review_score") == 5, "High")
        .otherwise("Unknown")
    )
     #Counting total number of valid reviews compute percentages
    valid_reviews_df= ratings_df.filter(col("rating_band")!= "Unknown")
    total_review_count =valid_reviews_df.count()

    print_section("Q3.Total valid reviews used for rating-band percentage")
    print("Total valid reviews:", total_review_count)

    # Grouping bands by rating
    grouped_bands = valid_reviews_df.groupBy("rating_band").agg(
        count("*").alias("total_reviews"),
        countDistinct("user_id").alias("distinct_users"),
        countDistinct("id").alias("distinct_books")
    )

    # adding % of total reviews and mean review by user
    final_distribution =(
        grouped_bands
        .withColumn("percentage_of_total_reviews",spark_round((col("total_reviews")/ lit(total_review_count)) * 100, 2))
        .withColumn("avg_reviews_per_user",spark_round(col("total_reviews") /col("distinct_users"),4))
    )

    #Sorting the table with the highest number of reviews first
    final_distribution =final_distribution.orderBy(col("total_reviews").desc())
    
    print_section("Q3.Rating distribution and user behaviour")
    final_distribution.show(truncate=False)

    #exporting output for plotting
    final_distribution.coalesce(1)\
        .write\
        .mode("overwrite")\
        .option("header","true")\
        .csv(output_path+ "/q3_rating_distribution")

    #Q4.Time-Based Review Analysis 
    #grouping by review year and half-year period to compare review activity across two parts of each year
    half_year_review_summary = (
        ratings_df.groupBy("review_year", "half_year")
            .agg(count("*").alias("total_reviews"),spark_round(avg("review_score"), 4).alias("avg_review_score"))
        .orderBy("review_year", "half_year")
    )

    print_section("Q4.Review counts and average scores by year and half-year")
    half_year_review_summary.show(10, truncate=False)

    #aggregating by year only
    yearly_reviews = (
        ratings_df.groupBy("review_year").agg(count("*").alias("total_reviews"),
                                              spark_round(avg("review_score"), 4).alias("avg_review_score"))
    )

    #window ordered by year for yearly comparison
    year_window = Window.orderBy("review_year")

    yearly_review_trends= (
        yearly_reviews
        .withColumn("previous_year_reviews",lag("total_reviews").over(year_window))
        .withColumn(
            "year_on_year_change",
            col("total_reviews") - col("previous_year_reviews"))
        .withColumn(
            "year_on_year_growth_percent",
            spark_round(
                (col("year_on_year_change") / col("previous_year_reviews")) * 100,2))
        .orderBy("review_year")
    )

    print_section("Q4.Year on year review count")
    yearly_review_trends.show(30, truncate=False)
    
    #Identifing the year with highest total review activity
    highest_activity_year = (
        yearly_review_trends
        .orderBy(col("total_reviews").desc())
        .limit(1)
    )

    print_section("Q4.Year with highest review activity")
    highest_activity_year.show(truncate=False)
    
    #year with the largest percentage growth
    # The first year does not have previous year hence growth is null and is not considered
    largest_growth_year = (
        yearly_review_trends
        .filter(col("year_on_year_growth_percent").isNotNull())
        .orderBy(col("year_on_year_growth_percent").desc())
        .limit(1)
    )

    print_section("Q4.Year with largest percentage growth in review count")
    largest_growth_year.show(truncate=False)

    # saving outputs for plotting 
    half_year_review_summary.coalesce(1)\
        .write\
        .mode("overwrite")\
        .option("header","true")\
        .csv(output_path +"/q4_half_year_review_summary")

    yearly_review_trends.coalesce(1)\
        .write\
        .mode("overwrite")\
        .option("header","true")\
        .csv(output_path + "/q4_yearly_review_trends")

    #Q5. Category Popularity and Review Behaviour

    #merging ratings with book metadata to get book category attached
    ratings_books_df =(ratings_df.join(books_df.select("id","title","categories"),on="id",how="left"))
    
    print_section("Q5.Joined ratings and books data")
    ratings_books_df.select("id","title","categories","user_id","review_score").show(10, truncate=False)

    #Grouping by category and computing
    category_summary = (
        ratings_books_df.groupBy("categories").agg(
            count("*").alias("total_reviews"),
            spark_round(avg("review_score"), 4).alias("avg_review_score"),
            countDistinct("user_id").alias("distinct_users"),
            countDistinct("id").alias("distinct_books"))
    )

    # Average reviews per book shows how much review activity each book receives in each cat
    category_summary = (
        category_summary.withColumn("avg_reviews_per_book",spark_round(col("total_reviews") / col("distinct_books"), 4))
        .orderBy(col("total_reviews").desc())
    )
    #top 10 categories by total number of reviews
    top10_categories_by_reviews=category_summary.limit(10)

    print_section("Q5. Top 10 categories by total number of reviews")
    top10_categories_by_reviews.show(10,truncate=False)

    #top-10 cats by average score 
    top10_categories_by_avg_score = (category_summary.orderBy(col("avg_review_score").desc(),
                                                              col("total_reviews").desc()).limit(10))

    top10_categories_by_avg_reviews_per_book = (
        category_summary.orderBy(col("avg_reviews_per_book").desc(), col("total_reviews").desc()).limit(10)
    )

    print_section("Q5. Top 10 categories by average review score")
    top10_categories_by_avg_score.show(10, truncate=False)

    print_section("Q5. Top 10 categories by average reviews per book")
    top10_categories_by_avg_reviews_per_book.show(10, truncate=False)

    #Saving outputs f
    top10_categories_by_reviews.coalesce(1)\
        .write\
        .mode("overwrite")\
        .option("header","true")\
        .csv(output_path +"/q5_top10_categories_by_reviews")

    top10_categories_by_avg_score.coalesce(1)\
        .write\
        .mode("overwrite")\
        .option("header", "true")\
        .csv(output_path +"/q5_top10_categories_by_avg_score")

    top10_categories_by_avg_reviews_per_book.coalesce(1) \
        .write\
        .mode("overwrite")\
        .option("header","true")\
        .csv(output_path + "/q5_top10_categories_by_avg_reviews_per_book")

    #Q6.Top Books by Rating and Popularity

    #Here I join the cleaned ratings table with the book metadata table using shared book id col
    ratings_with_titles=ratings_df.join(books_df.select("id","title"),on="id",how="left")

    # filling the title with unlu=oen title in case review does not match the title rather than droping the row
    ratings_with_titles =ratings_with_titles.fillna({"title":"Unknown Title"})

    print_section("Q6. Preview of ratings data merged with book titles")
    ratings_with_titles.select("id","title","user_id","review_score").show(10, truncate=False)

    #Summarising review behaviour at book-title level for total reviews,average score,and distinct users
    books_rating_summary = (
        ratings_with_titles.groupBy("title").agg(
            count("*").alias("total_reviews"),
            spark_round(avg("review_score"), 4).alias("avg_review_score"),
            countDistinct("user_id").alias("distinct_users"))
    )

    #Only keeping books with at least 50 reviews to avoids ranking books that have good score because of their few number of reviews
    books_with_enough_reviews=books_rating_summary.filter(col("total_reviews")>= 50)


    #if 2 books have same average score the one with more review ranks higher
    top10_books_by_rating = (books_with_enough_reviews.orderBy(col("avg_review_score")
        .desc(),col("total_reviews").desc()).limit(10)
    )

    print_section("Q6. Top 10 books by average score with at least 50 reviews")
    top10_books_by_rating.show(10, truncate=False)

    #Saving output
    top10_books_by_rating.coalesce(1)\
        .write\
        .mode("overwrite")\
        .option("header", "true")\
        .csv(output_path +"/q6_top10_books_by_rating")

    #Q7. Reviewer Activity Analysis

    #Q7 part a
    #checking the use activity by grouping by user_id and counting number of reviews
    reviewer_activity= (ratings_df.groupBy("user_id").agg(count("*").alias("review_count")))

    # categorsing reviewers by frequent and infrequent reviewers based on 50 reviews threshold 
    reviewer_activity =reviewer_activity.withColumn("reviewer_type",
        when(col("review_count")> 50,"Frequent Reviewer")
        .otherwise("Infrequent Reviewer")
    )

    # top 10 reviewers contribution to check wether most of reviews come from small number of users
    top10_reviewers=(reviewer_activity.orderBy(col("review_count").desc()).limit(10))

    print_section("Q7a.Top 10 most active reviewers")
    top10_reviewers.show(10,truncate=False)

    #Counting number of reviewer activities in each group.
    reviewer_distribution =(reviewer_activity.groupBy("reviewer_type").agg(count("*").alias("number_of_reviewers"))
        .orderBy(col("number_of_reviewers").desc())
    )

    print_section("Q7a.Reviewer distribution")
    reviewer_distribution.show(truncate=False)

    # Q7 part b
    # merging books_df to rating_df to attach book cats to reviews
    ratings_with_categories =ratings_df.join(books_df.select("id","categories"),on="id",how="left")

    # filling all books reviews with other where id does not match meta data file after merge
    ratings_with_categories =ratings_with_categories.fillna({"categories":"Other"})

    #counting how many diffrent users conributed to each category
    category_reviewers = (ratings_with_categories.groupBy("categories")
        .agg(countDistinct("user_id").alias("distinct_reviewers"))
        .orderBy(col("distinct_reviewers").desc())
    )

    print_section("Q7b.Category-level diffrent reviewer counts")
    category_reviewers.show(10,truncate=False)

    # Top 5 categorise with most reviewer base
    top5_categories_by_reviewers=category_reviewers.limit(5)

    print_section("Q7b.Top 5 categories by number of distinct reviewers")
    top5_categories_by_reviewers.show(5,truncate=False)

    # Save the outputs for the report and the required bar chart.
    category_reviewers.coalesce(1)\
        .write\
        .mode("overwrite")\
        .option("header", "true")\
        .csv(output_path +"/q7_category_reviewer_counts")

    top5_categories_by_reviewers.coalesce(1)\
        .write\
        .mode("overwrite")\
        .option("header", "true")\
        .csv(output_path +"/q7_top5_categories_by_reviewers")

    spark.stop()
