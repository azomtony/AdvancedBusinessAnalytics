from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce,lit, current_timestamp,year,month,dayofmonth,from_json,split, col, avg, count, udf, desc, when, to_timestamp,explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from textblob import TextBlob
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.recommendation import ALS,ALSModel
import pymongo

#Spark session with Kafka and MongDB integration
spark = SparkSession.builder \
    .appName("YelpAdvancedAnalytics") \
    .config("spark.mongodb.write.connection.uri", "mongodb://192.168.64.5:27017/yelp.analytics") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
    .getOrCreate()

#All schemas
review_schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("business_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("stars", DoubleType(), True),
    StructField("text", StringType(), True),
    StructField("date", StringType(), True)
])
checkin_schema = StructType([
    StructField("business_id", StringType(), True),
    StructField("date", StringType(), True)
])
business_schema = StructType([
    StructField("business_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("stars", DoubleType(), True),
    StructField("review_count", LongType(), True),
    StructField("categories", StringType(), True)
])
user_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("review_count", LongType(), True),
    StructField("yelping_since", StringType(), True),
    StructField("friends", StringType(), True),
    StructField("useful", LongType(), True),
    StructField("funny", LongType(), True),
    StructField("cool", LongType(), True)
])
tip_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("business_id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("date", StringType(), True),
    StructField("compliment_count", LongType(), True)
])

#consuming messages from Kafka
review_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.64.5:9092") \
    .option("subscribe", "yelp-review") \
    .option("startingOffsets", "earliest") \
    .load()
checkin_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.64.5:9092") \
    .option("subscribe", "yelp-checkin") \
    .option("startingOffsets", "earliest") \
    .load()
business_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.64.5:9092") \
    .option("subscribe", "yelp-businesses") \
    .option("startingOffsets", "earliest") \
    .load()
user_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.64.5:9092") \
    .option("subscribe", "yelp-user") \
    .option("startingOffsets", "earliest") \
    .load()
tip_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.64.5:9092") \
    .option("subscribe", "yelp-tip") \
    .option("startingOffsets", "earliest") \
    .load()

#parsing json data from kafka topic
review_df = review_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), review_schema).alias("data")).select("data.*")
checkin_df = checkin_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), checkin_schema).alias("data")).select("data.*")
business_df = business_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), business_schema).alias("data")).select("data.*")
user_df = user_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), user_schema).alias("data")).select("data.*")
review_df = review_df.withColumn("event_time", to_timestamp(col("date")))
review_df_with_watermark = review_df.withWatermark("event_time", "10 minutes")
tip_df = tip_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), tip_schema).alias("data")).select("data.*")
tip_df = tip_df.withColumn("event_time", to_timestamp(col("date")))
tip_df_with_watermark = tip_df.withWatermark("event_time", "10 minutes")
business_dfs = business_df.withColumn("timestamp", current_timestamp())
business_df_with_watermark = business_dfs.withWatermark("timestamp", "10 minutes")

#Sentiment analysis usint textblob to get review tone
def analyze_sentiment(text):
    analysis = TextBlob(text)
    if analysis.sentiment.polarity > 0:
        return 'Positive'
    elif analysis.sentiment.polarity == 0:
        return 'Neutral'
    else:
        return 'Negative'
    
sentiment_udf = udf(analyze_sentiment, StringType())
review_df_with_sentiment = review_df_with_watermark.withColumn("sentiment", sentiment_udf(review_df["text"]))
tip_df_with_sentiment = tip_df_with_watermark.withColumn("sentiment", sentiment_udf(tip_df["text"]))

#Processing data to send to MongoDB
sentiment_aggregation = review_df_with_sentiment.groupBy("business_id").agg(
    avg("stars").alias("average_rating"),
    count("review_id").alias("review_count"),
    count(when(col("sentiment") == "Positive", True)).alias("positive_reviews"),
    count(when(col("sentiment") == "Negative", True)).alias("negative_reviews")
)
popularity_aggregation = checkin_df.groupBy("business_id").count().alias("checkin_count")
geospatial_aggregation = business_df.select("business_id","name", "latitude", "longitude", "categories", "stars", "review_count")
user_engagement = user_df.select("user_id", "name", "review_count", "useful", "cool", "funny")
review_df_with_sentiments = review_df_with_sentiment \
    .withColumn("review_year", year("event_time")) \
    .withColumn("review_month", month("event_time")) \
    .withColumn("review_day", dayofmonth("event_time"))

category_distribution = business_df_with_watermark \
    .withColumn('category', explode(split(col('categories'), ', '))) \
    .groupBy("category") \
    .count().alias("category_count")

top_rated_categories = business_df_with_watermark \
    .withColumn('category', explode(split(col('categories'), ', '))) \
    .groupBy("category") \
    .agg(avg("stars").alias("average_rating")) \
    .orderBy(desc("average_rating"))

city_wise_business_count = business_df_with_watermark \
    .groupBy("city") \
    .count().alias("business_count")

review_trend_aggregation = review_df_with_sentiments.groupBy("review_year", "review_month", "business_id") \
    .agg(
        count("review_id").alias("review_count"),
        avg("stars").alias("average_rating"),
        count(when(col("sentiment") == "Positive", True)).alias("positive_reviews"),
        count(when(col("sentiment") == "Negative", True)).alias("negative_reviews")
    )

tip_sentiment_aggregation = tip_df_with_sentiment.groupBy("business_id") \
    .agg(
        count("text").alias("tip_count"),
        count(when(col("sentiment") == "Positive", True)).alias("positive_tips"),
        count(when(col("sentiment") == "Negative", True)).alias("negative_tips")
    )
#Writing processed data to MongDB collections
def write_to_mongodb(df, collection_name):
    client = pymongo.MongoClient("mongodb://192.168.64.5:27017")
    db = client["yelp"]
    collection = db[collection_name]
    batch_df = df.toPandas()
    collection.insert_many(batch_df.to_dict("records"))

#Queirying and sendig data to MongoDB and using checkpoint for fault tolerance
query_sentiment = sentiment_aggregation.writeStream \
    .foreachBatch(lambda df, _: write_to_mongodb(df, "sentiment_aggregation")) \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/checkpoint/sentiment_aggregation") \
    .start()

query_popularity = popularity_aggregation.writeStream \
    .foreachBatch(lambda df, _: write_to_mongodb(df, "popularity_aggregation")) \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/checkpoint/popularity_aggregation") \
    .start()

query_geospatial = geospatial_aggregation.writeStream \
    .foreachBatch(lambda df, _: write_to_mongodb(df, "geospatial_aggregation")) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint/geospatial_aggregation") \
    .start()

query_user = user_engagement.writeStream \
    .foreachBatch(lambda df, _: write_to_mongodb(df, "user_engagement")) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint/user_engagement") \
    .start()

query_category_distribution = category_distribution.writeStream \
    .foreachBatch(lambda df, _: write_to_mongodb(df, "category_distribution")) \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/checkpoint/category_distribution") \
    .start()

query_top_rated_categories = top_rated_categories.writeStream \
    .foreachBatch(lambda df, _: write_to_mongodb(df, "top_rated_categories")) \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/checkpoint/top_rated_categories") \
    .start()

query_city_wise_business_count = city_wise_business_count.writeStream \
    .foreachBatch(lambda df, _: write_to_mongodb(df, "city_wise_business_count")) \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/checkpoint/city_wise_business_count") \
    .start()

query_review_trend = review_trend_aggregation.writeStream \
    .foreachBatch(lambda df, _: write_to_mongodb(df, "review_trend_aggregation")) \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/checkpoint/review_trend_aggregation") \
    .start()

query_tip_sentiment = tip_sentiment_aggregation.writeStream \
    .foreachBatch(lambda df, _: write_to_mongodb(df, "tip_sentiment_aggregation")) \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/checkpoint/tip_sentiment_aggregation") \
    .start()

#Using MLlib for predictive analytics
als = ALS(userCol="indexedUser", itemCol="indexedItem", ratingCol="stars", coldStartStrategy="drop", nonnegative=True, implicitPrefs=False)
indexer_user = StringIndexer(inputCol="user_id", outputCol="indexedUser")
indexer_item = StringIndexer(inputCol="business_id", outputCol="indexedItem")

def process_batch(df, epoch_id):
    df = indexer_user.fit(df).transform(df)
    df = indexer_item.fit(df).transform(df)
    model = als.fit(df)
    predictions = model.transform(df)
    predictions_selected = predictions.select("user_id", "business_id", "prediction")
    write_to_mongodb(predictions_selected, "predictions")

query_als = review_df.writeStream.foreachBatch(process_batch).start()

query_sentiment.awaitTermination()
query_popularity.awaitTermination()
query_geospatial.awaitTermination()
query_user.awaitTermination()
query_category_distribution.awaitTermination()
query_top_rated_categories.awaitTermination()
query_city_wise_business_count.awaitTermination()
query_review_trend.awaitTermination()
query_tip_sentiment.awaitTermination()
query_als.awaitTermination()
