package com.trandyol.stream_processing

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, col, count, current_timestamp, from_json, lit, struct, to_json, window}
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}

import scala.tools.nsc.Properties

class RawDataStreamingSparkJob extends Serializable with LazyLogging{

  def process(): Unit = {

    val spark = SparkSession
      .builder
      .appName("Trendyol Streaming")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val orderSchema = new StructType()
      .add("customer_id", StringType)
      .add("location", StringType)
      .add("order_date", StringType)
      .add("order_id", StringType)
      .add("price", DoubleType)
      .add("product_id", StringType)
      .add("seller_id", StringType)
      .add("status", StringType)

    val ordersDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
      .option("subscribe", "orders")
      .option("startingOffsets", "earliest")
      .load()

    def ordersStream = ordersDF.select(from_json(col("value").cast("string"), orderSchema).alias("parsed_value"))
      .select("parsed_value.*").withColumn("timestamp", lit(current_timestamp()))

    def resultDf = ordersStream
      .withWatermark("timestamp", "20 seconds")
      .groupBy($"location", window($"timestamp", "5 minutes", "1 minutes").as("date"))
      .agg(approx_count_distinct("seller_id").as("seller_count"),
        count("product_id").as("product_count"))

    spark.sparkContext.setLogLevel("WARN")
    resultDf.select($"date".cast("string").as("key"),
      to_json(struct("seller_count", "location", "product_count")).as("value"))
      .writeStream
      .format("kafka")
      .outputMode("append")
      .option("kafka.bootstrap.servers", "localhost:29092")
      .option("topic", "processed_orders")
      .option("checkpointLocation", "/checkpoint")
      .start()
      .awaitTermination()
  }
}
