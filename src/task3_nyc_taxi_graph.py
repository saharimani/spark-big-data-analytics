"""
Task 3. Graph Analytics on NYC Yellow Taxi Data
"""

import os
import socket
import pytz #handling time zone
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from graphframes import GraphFrame
from pyspark.sql.functions import col, count, lit, round as spark_round, collect_set
from pyspark import StorageLevel

# helper function for timestamped screenshots
def print_section(title, value=None):
    print("\n" + "-" * 90)
    print(title)
    if value is not None:
        print(value)
    uk_time =datetime.now(pytz.timezone('Europe/London'))
    print("Terminal timestamp:",uk_time.strftime("%Y-%m-%d %H:%M:%S"))
    print("-" *90)

if __name__ == "__main__":
    
    #starting the Spark application
    spark= SparkSession\
        .builder\
        .appName("Graph_Analytics_on_NYC_Yellow_Taxi_Data")\
        .getOrCreate()
    #keeping terminal output focused on my own results rather than Spark system logs
    spark.sparkContext.setLogLevel("ERROR")

    print("Spark version:", spark.version)
    print("Driver running on:", socket.gethostname())

    #Bucket paths
    # On the cluster these come from the environment; for a local run set
    # TRIPS_PATH, ZONES_PATH, OUTPUT_PATH and CHECKPOINT_DIR instead (see README)
    bucket_name = os.environ.get("BUCKET_NAME", "")
    data_bucket = os.environ.get("DATA_REPOSITORY_BUCKET", "")

    #input paths from module bucket
    trips_path = os.environ.get("TRIPS_PATH") or "s3a://" + data_bucket + "/nyc_taxi/yellow_tripdata/2023/"
    zone_lookup_path = os.environ.get("ZONES_PATH") or "s3a://" + data_bucket + "/nyc_taxi/taxi_zone_lookup.csv"

    # Output path
    output_path = os.environ.get("OUTPUT_PATH") or "s3a://" + bucket_name + "/task3/final_output"

    print("Trips input path:",trips_path)
    print("Zone lookup input path:",zone_lookup_path)
    print("Output path:",output_path)

    #Q1. Graph Construction & Data Preparation

    #Loading the data
    raw_trips_df =spark.read.option("header","true").option("inferSchema","true").csv(trips_path)
    zones_df= spark.read.option("header","true").option("inferSchema","true").csv(zone_lookup_path)

    print_section("Q1. Raw taxi trips schema")
    raw_trips_df.printSchema()

    print_section("Q1. Raw taxi zone lookup schema")
    zones_df.printSchema()
    
    #raw trip count before cleaning
    raw_trip_count =raw_trips_df.count()
    
    print_section("Q1. Raw trip count before cleaning")
    print("Raw trip count:",raw_trip_count)

    # Checkin gmissing pickup and dropoff IDs before cleaning
    null_pickup_dropoff_count=(raw_trips_df.filter(col("PULocationID").isNull() | col("DOLocationID").isNull()).count())

    print_section("Q1. Rows with null pickup or dropoff IDs")
    print("Rows with null PULocationID or DOLocationID:",null_pickup_dropoff_count)
    
    #checking for missing or duplicate LocationID Before cleaning
    null_zone_id_count = zones_df.filter(col("LocationID").isNull()).count()

    duplicate_zone_id_count =(zones_df.groupBy("LocationID").count().filter(col("count") > 1).count())

    print_section("Q1.Zone lookup ID quality checks")
    print("Rows with null LocationID:",null_zone_id_count)
    print("Duplicated LocationID values:",duplicate_zone_id_count)

    # Prepareing Vertices-The Taxi Zones
    # renamingLocationID to 'id' as required by GraphFrames and casting it to an integer
    vertices_df =(zones_df.select(col("LocationID").cast("int").alias("id"),
                                   col("Borough"),
                                   col("Zone"),
                                   col("service_zone")))

    #preparing Edges-The Taxi Trips
    #renaming pickup to 'src' and dropoff to 'dst' and casting them to integers, and selecting metrics
    edges_df=(raw_trips_df.select(col("PULocationID").cast("int").alias("src"),
                                    col("DOLocationID").cast("int").alias("dst"),
                                    col("trip_distance").cast("float"),
                                    col("total_amount").cast("float")).dropna(subset=["src","dst"]))
    
    #Caching the dataframe
    vertices_df.cache()
    edges_df.cache()
    
    #Constructing the GraphFrame
    taxi_graph =GraphFrame(vertices_df,edges_df)

    #Computing the required counts
    number_of_vertices= taxi_graph.vertices.count()
    number_of_edges= taxi_graph.edges.count()

    print_section("Q1.Vertices Preview")
    taxi_graph.vertices.show(10,truncate=False)

    print_section("Q1.Edges Preview")
    taxi_graph.edges.show(10,truncate=False)

    #Summary table 
    q1_summary_df = spark.createDataFrame([("Raw trip count", raw_trip_count),
                                           ("Number of vertices",number_of_vertices),
                                           ("Number of edges",number_of_edges)],
                                          ["metric", "value"])

    print_section("Q1. Graph construction summary")
    q1_summary_df.show(truncate=False)

    #Q2. Graph Connectivity & Degree Analysis
    
    # duplicate trips do not change graph connectivity but they make graph algorithms much heavier on the cluster so duplicates are dropped
    route_edges_df=edges_df.select("src","dst").dropDuplicates()
    route_graph=GraphFrame(vertices_df, route_edges_df)

    print_section("Q2. Route-level graph edge count")
    print("Unique route edges:", route_edges_df.count())
    
    #Q2a. Connected Component
    
    # GraphFrames needs checkpoint directory to run
    checkpoint_path = os.environ.get("CHECKPOINT_DIR") or "s3a://" + bucket_name + "/task3_checkpoints"
    spark.sparkContext.setCheckpointDir(checkpoint_path)
    
    #Running connected components algorithm 
    components_df=route_graph.connectedComponents()

    print_section("Q2a. Connected components sample")
    components_df.show(10,truncate=False)

    #Grouping by component ID to count zones belonging to each component
    component_sizes_df =(components_df.groupBy("component").agg(count("*").alias("size")).orderBy(col("size").desc()))

    #computing total number of connected components
    total_components =component_sizes_df.count()
    
    print_section("Q2a. Total number of connected components")
    print("Total components:",total_components)
    
    print_section("Q2a. Top component sizes")
    component_sizes_df.show(5,truncate=False)

    #saving output
    component_sizes_df.write\
        .mode("overwrite")\
        .option("header", "true")\
        .csv(output_path +"/q2a_component_statistics")

    # Q2b. Degree & Triangle Analysis
    #Computing Degrees using GraphFrames builtin functions
    in_degree_df =taxi_graph.inDegrees.withColumnRenamed("inDegree","in_degree") # edges arrive at each taxi zone
    out_degree_df =taxi_graph.outDegrees.withColumnRenamed("outDegree","out_degree")# edges leave each taxi zone
    total_degree_df= taxi_graph.degrees.withColumnRenamed("degree", "total_degree") #total degree for each zone

    #Joining three degree outputs with vertex metadata
    degree_summary_df =(taxi_graph.vertices.join(in_degree_df,on="id",how="left")
        .join(out_degree_df, on="id",how="left")
        .join(total_degree_df,on="id",how="left")
        .fillna({"in_degree": 0,"out_degree": 0,"total_degree": 0}))
    
    top_10_degrees = (degree_summary_df.select("id", "Borough","Zone",
                                               "service_zone", "in_degree",
                                               "out_degree", "total_degree")
        .orderBy(col("total_degree").desc())
        .limit(10)
    )

    print_section("Q2b. Top 10 zones by total degree")
    top_10_degrees.show(truncate=False)

    # Triangle count finds closed three-zone route patterns in the graph.
    triangles_df = route_graph.triangleCount(StorageLevel.MEMORY_AND_DISK)

    #Joining with vertices again to make the output readable
    top_10_triangles = (
    triangles_df.select("id","Borough","Zone","service_zone",col("count").alias("triangle_count"))
    .orderBy(col("triangle_count").desc())
    .limit(10)
    )

    print_section("Q2b. Top 10 zones by triangle count")
    top_10_triangles.show(10, truncate=False)


    #Q3. Shortest Path & Reachability
    
    #edge filtering- filtering out impossible trips with zero or negative distance or cost
    filtered_edges_df = taxi_graph.edges.filter((col("trip_distance") > 0) & (col("total_amount") > 0))

    #making the cleaned GraphFrame
    cleaned_taxi_graph=GraphFrame(taxi_graph.vertices,filtered_edges_df)

    #Defining the start and goal zones
    # Start:161(Midtown Center,Manhattan)
    # Goal:132(JFK Airport,Queens)
    start_id=161
    goal_id=132

    # BFS Shortest Path- maximum depth of 4
    bfs_path_df = cleaned_taxi_graph.bfs(fromExpr=f"id = {start_id}",
                                         toExpr=f"id = {goal_id}",
                                         maxPathLength=4)

    print_section("Q3. BFS Shortest Path (Midtown Center to JFK)")
    
    #showing 1 row due to limited space
    bfs_path_df.show(1,truncate=False)

    # Multi-destination Reachability:calculating shortest path in hops from all zones to JFK Airport
    #passing goal_id for shortestPaths alogrithem
    reachability_df=cleaned_taxi_graph.shortestPaths(landmarks=[goal_id])

    # the result created dictionary/map column called distances with specific hop count to JFK
    reachability_clean_df = (reachability_df.withColumn("hops_to_jfk", col("distances").getItem(goal_id))
        .select("id", "Borough", "Zone", "hops_to_jfk")
        .filter(col("hops_to_jfk").isNotNull())## Dropping zones that cannot reach JFK
        .orderBy(col("hops_to_jfk").desc()) #Ordering descending to show the furthest zones
    )

    print_section("Q3. Sample Shortest Path Distances to JFK")
    reachability_clean_df.show(10,truncate=False)

    #Q4

    #Q4a. Classic PageRank
    #deduplicated route graph means each pickup-dropoff route appears once  even if many taxi trips happened on that same route.
    unique_route_edges_df=(edges_df.select("src","dst").dropDuplicates())

    print_section("Q4a. Number of unique routes used for classic PageRank")
    print("Unique route count:",unique_route_edges_df.count())

    #Building route-level graph rather than  using every trip as an edge
    classic_pagerank_graph=GraphFrame(vertices_df,unique_route_edges_df)

    # Applying GraphFrames PageRank with parameters that are specfied
    classic_pagerank_results=classic_pagerank_graph.pageRank(resetProbability=0.15,tol=0.01)

    #Ranking zones by PageRank score and showing top zones
    top_10_classic_pagerank = (classic_pagerank_results.vertices.select("id","Borough",
                                                                        "Zone","service_zone",
                                                                        spark_round(col("pagerank"), 6).alias("pagerank"))
        .orderBy(col("pagerank").desc())
        .limit(10)
    )

    print_section("Q4a.Top 10 zones by classic PageRank")
    top_10_classic_pagerank.show(10, truncate=False)

    #Q4b. Frequency-Aware PageRank by Trip Frequency

    #calculating route frequency: grouping by source and destination, and counting the total number of trips
    route_frequency_df = (taxi_graph.edges.groupBy("src", "dst")
        .agg(count("*").alias("route_frequency"))
        .orderBy(col("route_frequency").desc())
    )
    
    print_section("Q4b. Top 10 routes by trip frequency")
    route_frequency_df.show(10, truncate=False)

    # original trip level edges are used to keep repeated routes, this is  used because i tried weightCol in PageRank but  GraphFrames version on the cluster does not support it
    frequency_edges_df =taxi_graph.edges.select("src","dst")

    print_section("Q4b. Number of trip-level edges used for frequency-aware PageRank")
    print("Trip-level edge count:",frequency_edges_df.count())

    #creating frequency aware graph using repeated trip-level edges
    frequency_graph=GraphFrame(taxi_graph.vertices,frequency_edges_df)
    
    #Applying PageRank on the trip level graph,repeated routes remain repeated hence high-frequency routes have more influence.
    print("Running Frequency-Aware PageRank")
    freq_pagerank_results = frequency_graph.pageRank(resetProbability=0.15,tol=0.01)


    # Show the top 10 zones by frequency-aware PageRank.
    top_10_freq_pagerank =(freq_pagerank_results.vertices
        .select("id","Borough","Zone","service_zone",spark_round(col("pagerank"),6).alias("pagerank"))
        .orderBy(col("pagerank").desc())
        .limit(10)
    )

    print_section("Q4b. Top 10 Zones by Frequency-Aware PageRank")
    top_10_freq_pagerank.show(truncate=False)
    
    #Q5. Zone-Level Community Detection and Borough Composition
    
    #Applying Label Propagation Algorithm (LPA): I use maxIter=5 to avoid cluster timing out and should be enough  to form comunities
    print("Running Label Propagation Algorithm (LPA)")
    lpa_result_df=taxi_graph.labelPropagation(maxIter=5)

    #number of found communities
    num_communities = lpa_result_df.select("label").distinct().count()
    
    print_section("Q5. Total number of detected communities")
    print(f"Total communities:{num_communities}")

    #Top 5 largest communities by counting how many zones belong to each label
    community_sizes_df=(lpa_result_df.groupBy("label").agg(count("*").alias("size")).orderBy(col("size").desc()))
    
    print_section("Q5. Top 5 largest communities")
    community_sizes_df.show(5,truncate=False)

    #Mapping Community ID -> Boroughs, using collect_set to create list of all boroughs in each community
    borough_composition_df = (lpa_result_df.groupBy("label").agg(count("*").alias("size"),
                                                                 collect_set("Borough").alias("borough_composition"))
        .orderBy(col("size").desc())
    )
    
    print_section("Q5. Borough Composition of Top Communities")
    borough_composition_df.show(5, truncate=False)

    # Top 5 hubs in the LARGEST community, first extracting the label ID of biggest community
    largest_community_label=community_sizes_df.first()["label"]

    #merging LPA results with the total_degree_df created back in Q2b to find out which zones is main hubs for this community
    hubs_df = (lpa_result_df.filter(col("label") == largest_community_label)
        .join(total_degree_df,"id")
        .select("id","Borough", "Zone","total_degree", "label") 
        .orderBy(col("total_degree").desc())                
        .limit(5)
    )
    
    print_section(f"Q5. Top 5 Hubs in the Largest Community (Label: {largest_community_label})")
    hubs_df.show(truncate=False)

    spark.stop()