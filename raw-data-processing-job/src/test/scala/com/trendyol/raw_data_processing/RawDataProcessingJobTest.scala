package com.trendyol.raw_data_processing

import com.trendyol.raw_data_processing.job.RawDataProcessingJob
import org.apache.spark.sql.{SQLImplicits, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class RawDataProcessingJobTest extends AnyFlatSpec with BeforeAndAfterAll with Matchers {
  private val master = "local[1]"
  private implicit val appName: String = "spark-test"
  private var spark: SparkSession = _
  private lazy val sqlImplicits: SQLImplicits = spark.implicits
  private lazy val job = new RawDataProcessingJob(null, null, null, spark)

  override protected def beforeAll(): Unit = {
    spark = SparkSession.builder().appName(appName).config("spark.sql.shuffle.partitions", "1").master(master).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override protected def afterAll(): Unit = {
    spark.stop()
  }

  "getCalculatedBalance" should "give us the latest values of net_sales_amount net_sales_price,gross_sales_amount" +
    "gross_sales_price and avg_sales_amount_last_5_selling_days per product_id" in {
    import sqlImplicits._
    val ordersDf = Seq(
      ("2022-01-01T11:33:22.000+03:00", "product_id1", "İzmir", "Created", 1.0),
      ("2022-01-02T10:30:20.000+03:00", "product_id1", "Ankara", "Created", 1.0),

      ("2022-01-02T09:01:01.000+03:00", "product_id2", "İzmir", "Created", 8.0),
      ("2022-01-03T23:01:01.000+03:00", "product_id2", "İstanbul", "Created", 8.0),
      ("2022-01-04T22:10:01.000+03:00", "product_id2", "İstanbul", "Created", 7.0),
      ("2022-01-05T22:11:01.000+03:00", "product_id2", "İstanbul", "Created", 4.0),
      ("2022-01-16T15:01:01.000+03:00", "product_id2", "Ankara", "Created", 2.0),
      ("2022-01-17T15:23:01.000+03:00", "product_id2", "İzmir", "Returned", 2.0),

      ("2022-01-01T01:01:01.000+03:00", "product_id3", "İzmir", "Created", 3.0),
      ("2022-01-02T09:01:01.000+03:00", "product_id3", "İzmir", "Created", 3.0),
      ("2022-01-03T10:01:01.000+03:00", "product_id3", "Ankara", "Created", 4.0),
      ("2022-01-09T11:01:01.000+03:00", "product_id3", "Muğla", "Created", 4.0),
      ("2022-01-15T13:01:01.000+03:00", "product_id3", "Muğla", "Created", 5.0),
      ("2022-01-26T19:01:01.000+03:00", "product_id3", "Muğla", "Created", 6.0),

      ("2022-01-01T01:01:01.000+03:00", "product_id4", "İzmir", "Created", 3.0)
    ).toDF("order_date", "product_id", "location", "status", "price")

    val expectedResult = Seq(
      ("product_id1", 2, 2.0, 2, 2.0, 1.0),
      ("product_id2", 5, 29.0, 6, 31.0, 4.6),
      ("product_id3", 6, 25.0, 6, 25.0, 4.4),
      ("product_id4", 1, 3.0, 1, 3.0, 3.0)
    ).toDF("product_id", "net_sales_amount", "net_sales_price","gross_sales_amount", "gross_sales_price", "avg_sales_amount_last_5_selling_days")

    val result = job.calculateSalesAggregate(ordersDf)

    result.collect() should contain theSameElementsAs expectedResult.collect()
  }
  "calculatePriceBehavior" should "give us the the value of price change " in {
    import sqlImplicits._
    val ordersDf = Seq(
      ("2022-01-01T11:33:22.000+03:00", "product_id1", "İzmir", "Created", 1.0),
      ("2022-01-02T10:30:20.000+03:00", "product_id1", "Ankara", "Created", 1.0),
      ("2022-01-02T09:01:01.000+03:00", "product_id2", "İzmir", "Created", 8.0),
      ("2022-01-03T23:01:01.000+03:00", "product_id2", "İstanbul", "Created", 8.0),
      ("2022-01-04T22:10:01.000+03:00", "product_id2", "İstanbul", "Created", 7.0),
      ("2022-01-05T22:11:01.000+03:00", "product_id2", "İstanbul", "Created", 8.0),
      ("2022-01-16T15:01:01.000+03:00", "product_id2", "Ankara", "Created", 2.0),
      ("2022-01-17T15:23:01.000+03:00", "product_id2", "İzmir", "Returned", 2.0),
      ("2022-01-01T01:01:01.000+03:00", "product_id3", "İzmir", "Created", 3.0),
      ("2022-01-02T09:01:01.000+03:00", "product_id3", "İzmir", "Created", 3.0),
      ("2022-01-03T10:01:01.000+03:00", "product_id3", "Ankara", "Created", 4.0),
      ("2022-01-09T11:01:01.000+03:00", "product_id3", "Muğla", "Created", 4.0),
      ("2022-01-15T13:01:01.000+03:00", "product_id3", "Muğla", "Created", 5.0),
      ("2022-01-26T19:01:01.000+03:00", "product_id3", "Muğla", "Created", 6.0),
      ("2022-01-01T01:01:01.000+03:00", "product_id4", "İzmir", "Created", 3.0)
    ).toDF("order_date", "product_id", "location", "status", "price")

    val expectedResult = Seq(
      ( "product_id1", "2022-01-01T11:33:22.000+03:00",   "rice", 1.0),
      ( "product_id2",  "2022-01-02T09:01:01.000+03:00",  "rice", 8.0),
      ( "product_id2",  "2022-01-04T22:10:01.000+03:00",  "fall", 7.0),
      ( "product_id2",  "2022-01-05T22:11:01.000+03:00",  "rice", 8.0),
      ( "product_id2",  "2022-01-16T15:01:01.000+03:00",  "fall", 2.0),
      ( "product_id3",  "2022-01-01T01:01:01.000+03:00",  "rice", 3.0),
      ( "product_id3",  "2022-01-03T10:01:01.000+03:00",  "rice", 4.0),
      ( "product_id3",  "2022-01-15T13:01:01.000+03:00",  "rice", 5.0),
      ( "product_id3",  "2022-01-26T19:01:01.000+03:00",  "rice", 6.0),
      ( "product_id4", "2022-01-01T01:01:01.000+03:00",   "rice", 3.0)
    ).toDF("product_id", "order_date", "change", "price")

    val result = job.calculatePriceBehavior(ordersDf)
    result.collect() should contain theSameElementsAs expectedResult.collect()
  }
}