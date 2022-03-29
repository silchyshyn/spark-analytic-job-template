package com.trendyol.raw_data_processing.job

import com.trendyol.raw_data_processing.config.{DatasetConf, NoDatasetConfException, StructuringConfig}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, desc, lag, lit, lower, row_number, sum, when}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class RawDataProcessingJob(
                             private val config: StructuringConfig,
                             private val ordersSourcePath: String,
                             private  val productSourceFile: String,
                             @transient private val spark: SparkSession) extends Serializable with LazyLogging {
  import spark.implicits._
  def process(): Unit = {
    val ordersDatasetConf = getDatasetDefinition("orders")
    val productsDatasetConf = getDatasetDefinition("products")
    spark.conf.set("spark.sql.shuffle.partitions", 8)
    val ordersDf = spark.read.schema(buildDataFrameSchema(ordersDatasetConf.datasetSchema)).json(ordersSourcePath)
    val columnsList = ordersDf.columns
    val ordersCleaned = ordersDf.dropDuplicates(columnsList)

    val salesAggregateDF = calculateSalesAggregate(ordersCleaned)
    val topSellingLocationDF = calculateTopLocation(ordersCleaned)


    val problem1ResultDF = salesAggregateDF.as("salesAggregate")
      .join(topSellingLocationDF.as("topSellingLocation"), Seq("product_id"), "inner")
      .select($"salesAggregate.product_id",
        $"salesAggregate.net_sales_amount",
        $"salesAggregate.net_sales_price",
        $"salesAggregate.gross_sales_amount",
        $"salesAggregate.gross_sales_price",
        $"salesAggregate.avg_sales_amount_last_5_selling_days",
        $"topSellingLocation.location".as("top_selling_location"))

    problem1ResultDF.coalesce(1)
      .write.mode("overwrite")
      .format(config.targetFileFormat)
      .save(config.targetPath)

    val problem2ResultDF = calculatePriceBehavior(ordersCleaned).drop("previous_price")

    problem2ResultDF.coalesce(1)
      .write.mode("overwrite")
      .format(config.targetFileFormat)
      .save(config.targetPriceBehaviorPath)


    val productsDF = spark.read.schema(buildDataFrameSchema(productsDatasetConf.datasetSchema)).json(productSourceFile)
    productsDF.coalesce(1)
      .write.mode("overwrite")
      .format(config.targetFileFormat)
      .save(config.targetProductsPath)
  }

    def getDatasetDefinition(dataset: String): DatasetConf = {
      config.sourceDatasets
        .find(x => x.dataset == dataset
        )
        .getOrElse(throw new NoDatasetConfException(s"Dataset conf for $dataset not found in the dataset structuring configuration"))
    }

    def calculateSalesAggregate(ordersCleaned: DataFrame): DataFrame = {
      val windowExpr = Window.partitionBy($"product_id").orderBy( desc("order_date"))
      ordersCleaned
        .withColumn("net_sales_price", when(lower(col("status")) === "created", col("price"))
          .otherwise(0))
        .withColumn("net_sales_amount", when(lower(col("status")) === "created", 1)
          .otherwise(0))
        .withColumn("desc_row_num_within_product", row_number().over(windowExpr))
        .withColumn("avg_within_product", avg(col("price")).over(windowExpr))
        .withColumn("salesAmount", count("*").over(Window.partitionBy($"product_id")))
        .withColumn("avg_sales_amount_last_5_selling_days",
          when($"desc_row_num_within_product" === 5, $"avg_within_product")
            .when($"desc_row_num_within_product" < 5 &&
              $"desc_row_num_within_product" === $"salesAmount", $"avg_within_product")
            .otherwise(0))
        .groupBy("product_id")
        .agg(sum($"net_sales_amount").as("net_sales_amount"),
          sum($"net_sales_price").as("net_sales_price"),
          count("*").as("gross_sales_amount"),
          sum($"price").as("gross_sales_price"),
          sum($"avg_sales_amount_last_5_selling_days").as("avg_sales_amount_last_5_selling_days")
        )
    }

  def calculateTopLocation(ordersCleaned: DataFrame): DataFrame = {
    ordersCleaned.groupBy("location", "product_id")
    .agg(count("location").as("location_cnt"))
    .withColumn("top_location",
    row_number().over(Window.partitionBy($"product_id").orderBy( desc("location_cnt"))))
    .filter($"top_location" === 1)
  }

  def calculatePriceBehavior(ordersCleaned: DataFrame): DataFrame = {
    ordersCleaned
      .withColumn("previous_price", lag("price", 1, 0)
        .over(Window.partitionBy($"product_id").orderBy("order_date")))
      .filter($"price" =!= $"previous_price")
      .withColumn("change", when($"price" > $"previous_price", lit("rice"))
        .otherwise(lit("fall")))
    .select($"product_id", $"order_date", $"change", $"price")
  }

    def buildDataFrameSchema(sourceDatasetSchema: Map[String, DataType]): StructType = {
      val dataColumns = sourceDatasetSchema.keys.toSeq
        .map(c => StructField(c, sourceDatasetSchema(c)))
      StructType(dataColumns)
    }
}

