import json
import pandas as pd
import numpy as np
import math

from datetime import datetime
from random import sample 

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType

sparkSession = (SparkSession
 .builder
 .appName('example-pyspark-read-and-write-from-hive')
 .enableHiveSupport()
 .getOrCreate())

# from pyspark.sql.functions import *
# from pyspark.sql.types import *

# # Convenience function for turning JSON strings into DataFrames.
# def jsonToDataFrame(json, schema=None):
#   # SparkSessions are available with Spark 2.0+
#   reader = spark.read
#   if schema:
#     reader.schema(schema)
#   return reader.json(sc.parallelize([json]))

# events = jsonToDataFrame("""
# {"query":"","page":{"current":1,"size":10},"filters":{"all":[{"crawled_date":"20151002"},{"host_is_superhost":"t"},{"instant_bookable":["t","f"]},{"room_type":"Private room"},{"minimum_nights":[1,2,3,4,5]},{"availability_30":{"from":5}},{"accommodates":{"from":2}}],"none":[{"maximum_nights":[1,2,3,4]}]},"sort":[{"_score":"desc"},{"overall_rating":"desc"}]}
# """)

# events.printSchema()
# root
#  |-- filters: struct (nullable = true)
#  |    |-- all: array (nullable = true)
#  |    |    |-- element: struct (containsNull = true)
#  |    |    |    |-- accommodates: struct (nullable = true)
#  |    |    |    |    |-- from: long (nullable = true)
#  |    |    |    |-- availability_30: struct (nullable = true)
#  |    |    |    |    |-- from: long (nullable = true)
#  |    |    |    |-- crawled_date: string (nullable = true)
#  |    |    |    |-- host_is_superhost: string (nullable = true)
#  |    |    |    |-- instant_bookable: array (nullable = true)
#  |    |    |    |    |-- element: string (containsNull = true)
#  |    |    |    |-- minimum_nights: array (nullable = true)
#  |    |    |    |    |-- element: long (containsNull = true)
#  |    |    |    |-- room_type: string (nullable = true)
#  |    |-- none: array (nullable = true)
#  |    |    |-- element: struct (containsNull = true)
#  |    |    |    |-- maximum_nights: array (nullable = true)
#  |    |    |    |    |-- element: long (containsNull = true)
#  |-- page: struct (nullable = true)
#  |    |-- current: long (nullable = true)
#  |    |-- size: long (nullable = true)
#  |-- query: string (nullable = true)
#  |-- sort: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- _score: string (nullable = true)
#  |    |    |-- overall_rating: string (nullable = true)


# Schema for UI event (generated using the function above)
request_schema = StructType([
      StructField('query', StringType(), True),
      StructField('page', StructType([
	             StructField('current', IntegerType(), True),
	             StructField('size', IntegerType(), True)
             ]), True),
      StructField('filters', StructType([
             StructField("all", ArrayType(StructType([
	             StructField('crawled_date', StringType(), True),
	             StructField('host_is_superhost', StringType(), True),
	             StructField('instant_bookable', ArrayType(StringType()), True),
	             StructField('room_type', StringType(), True),
	             StructField('minimum_nights', ArrayType(IntegerType()), True),
	             StructField('availability_30', StructType([
	             	StructField('from', StringType(), True)
	             ])),
	             StructField('accommodates', StructType([
	             	StructField('from', StringType(), True)
	             ]))
             ])), True),
             StructField("none", ArrayType(StructType([
	             StructField('maximum_nights', ArrayType(IntegerType()), True)
             ])), True)
         ])),
      StructField("sort", ArrayType(StructType([
	             StructField('_score', IntegerType(), True),
	             StructField('overall_rating', IntegerType(), True)
             ])), True)
  ])


clean_request_schema = StructType([
      StructField('query', StringType(), True),
      StructField('page', StructType([
	             StructField('current', IntegerType(), True),
	             StructField('size', IntegerType(), True)
             ]), True),
      StructField('filters', StructType([
	             StructField('crawled_date', StringType(), True),
	             StructField('host_is_superhost', ArrayType(StringType()), True),
	             StructField('instant_bookable', ArrayType(StringType()), True),
	             StructField('room_type', StringType(), True),
	             StructField('minimum_nights', ArrayType(IntegerType()), True),
	             StructField('availability_30', StructType([
	             	StructField('from', StringType(), True)
	             ])),
	             StructField('accommodates', StructType([
	             	StructField('from', StringType(), True)
	             ])),
	             StructField('maximum_nights', ArrayType(IntegerType()), True),
	             StructField('_score', StringType(), True),
	             StructField('overall_rating', StringType(), True)
         ]), True)
  ])

# Define custom schema
result_schema = StructType([
      StructField('meta', StructType([
             StructField('page', StructType([
	             StructField('current', StringType(), True),
	             StructField('total_pages', StringType(), True),
	             StructField('total_results', StringType(), True),
	             StructField('size', StringType(), True)
             ])),
             StructField('engine', StructType([
             	StructField('name', StringType(), True)
             ])),
             StructField('request_id', StringType(), True)
             ])),
      StructField("results", ArrayType(StructType([
             StructField('_meta', StructType([
	             StructField('score', StringType(), True)
             ])),
             StructField('id', StructType([
             	StructField('raw', StringType(), True)
             ])),
             StructField('name', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('availability_30', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('listing_url', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('host_identity_verified', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('review_scores_cleanliness', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('availability_365', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('review_scores_accuracy', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('review_scores_location', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('review_scores_value', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('scrape_id', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('accommodates', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('last_scraped', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('room_type', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('maximum_nights', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('calendar_updated', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('crawled_date', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('review_scores_rating', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('review_scores_communication', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('review_scores_checkin', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('availability_90', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('minimum_nights', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('instant_bookable', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('availability_60', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('host_id', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('first_review', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('index', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('last_review', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('host_is_superhost', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('guests_included', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('overall_rating', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('is_business_travel_ready', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('cancellation_policy', StructType([
             	StructField('snippet', StringType(), True)
             ])),
             StructField('price', StructType([
             	StructField('snippet', StringType(), True)
             ])),
         ])), True)
  ])

# API log parameters
#location = "boston"
location = "geneva"

index_name = "airbnb-history-" + location

# read all files from a folder
df = spark.read.json("../log/" + index_name + "/*.json")

search_api = "/api/as/v1/engines/" + index_name + "/search"

df2 = df.select(explode("results").alias("record")).filter(col("record.full_request_path") == search_api)

search_logs = df2.select("record.full_request_path", "record.timestamp", regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace("record.request_body", "\"all\":\[\{", ""), "\},\{", ","), "\}\],\"none\":\[\{", ","), "\}\]\},\"sort\":\[\{", ","), "\"desc\"\}\]\}", "\"desc\"\}\}").alias("request_body"), "record.response_body")

search_logs.toPandas().to_csv(index_name + "-raw-search-logs.csv")

# rankedResults = search_logs.withColumn("request", from_json(col('request_body'), request_schema)).withColumn('data', from_json(col('response_body'), result_schema)).select("timestamp", col("request.query").alias("query"), col("request.filters.all.crawled_date").alias("filter_crawled_date"), col("request.filters.all.host_is_superhost").alias("filter_host_is_superhost"), col("request.filters.all.instant_bookable").alias("filter_instant_bookable"), col("request.filters.all.room_type").alias("filter_room_type"), col("request.filters.all.minimum_nights").alias("filter_minimum_nights"), col("request.filters.all.availability_30.from").alias("filter_availability_30"), col("request.filters.all.accommodates.from").alias("filter_accommodates"), col("request.filters.none.maximum_nights").alias("filter_maximum_nights"), col("request.sort").alias("sort"), col("data.meta.request_id").alias("search_id"), col("data.meta.page.current").alias("paginated_id"), col("data.meta.page.total_pages").alias("total_pages"), col("data.meta.page.total_results").alias("total_results"), col("data.meta.page.size").alias("result_size"), posexplode("data.results"))

rankedResults = search_logs.withColumn("request", from_json(col('request_body'), clean_request_schema)).withColumn('data', from_json(col('response_body'), result_schema)).select("timestamp", col("request.query").alias("query"), col("request.filters.crawled_date").alias("filter_crawled_date"), col("request.filters.host_is_superhost").alias("filter_host_is_superhost"), col("request.filters.instant_bookable").alias("filter_instant_bookable"), col("request.filters.room_type").alias("filter_room_type"), col("request.filters.minimum_nights").alias("filter_minimum_nights"), col("request.filters.availability_30.from").alias("filter_availability_30"), col("request.filters.accommodates.from").alias("filter_accommodates"), col("request.filters.maximum_nights").alias("filter_maximum_nights"), col("request.filters._score").alias("_score"), col("request.filters.overall_rating").alias("overall_rating"), col("data.meta.request_id").alias("search_id"), col("data.meta.page.current").alias("paginated_id"), col("data.meta.page.total_pages").alias("total_pages"), col("data.meta.page.total_results").alias("total_results"), col("data.meta.page.size").alias("result_size"), posexplode("data.results"))

impressions = rankedResults.select("timestamp", "query", "filter_crawled_date", "filter_host_is_superhost", "filter_instant_bookable", "filter_room_type", "filter_minimum_nights", "filter_availability_30", "filter_accommodates", "filter_maximum_nights", "search_id", "paginated_id", "total_pages", "total_results", "result_size", (col("pos")+1).alias("position"), col("col._meta.score").alias("score"), col("col.id.raw").alias("doc_id"), col("col.index.snippet").alias("index"), col("col.name.snippet").alias("title"), col("col.availability_30.snippet").alias("availability_30"), col("col.listing_url.snippet").alias("listing_url"), col("col.review_scores_rating.snippet").alias("review_scores_rating"), col("col.review_scores_cleanliness.snippet").alias("review_scores_cleanliness"), col("col.review_scores_communication.snippet").alias("review_scores_communication"), col("col.review_scores_checkin.snippet").alias("review_scores_checkin"), col("col.availability_90.snippet").alias("availability_90"), col("col.availability_365.snippet").alias("availability_365"), col("col.review_scores_accuracy.snippet").alias("review_scores_accuracy"), col("col.review_scores_location.snippet").alias("review_scores_location"), col("col.review_scores_value.snippet").alias("review_scores_value"), col("col.availability_60.snippet").alias("availability_60"), col("col.scrape_id.snippet").alias("scrape_id"), col("col.last_scraped.snippet").alias("last_scraped"), col("col.host_id.snippet").alias("host_id"), col("col.first_review.snippet").alias("first_review"), col("col.last_review.snippet").alias("last_review"), col("col.overall_rating.snippet").alias("overall_rating"), col("col.price.snippet").alias("price"))

impressions.toPandas().to_csv(index_name + '-impressions.csv')

