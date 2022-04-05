package com.trandyol.stream_processing

import com.trandyol.stream_processing.config.Order
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.json4s.{DefaultFormats, JValue}
import org.json4s.native.JsonMethods
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger
import org.apache.spark.sql.catalyst.plans.logical.Aggregate


object RawDataStreaming {


  implicit val typeInfo = TypeInformation.of(classOf[(String)])
  implicit lazy val jValue = TypeInformation.of(classOf[JValue])
  implicit  val orders = TypeInformation.of(classOf[Order])
  implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats


  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source = KafkaSource.builder()
      .setBootstrapServers("localhost:29092")
      .setTopics("orders")
      .setGroupId("orders-group1")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema)
      .build()

    val kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSource")

    val messageStream = kafkaStream
      .flatMap( rawData => JsonMethods.parse(rawData).toOption)
     .map(_.extract[Order])

/*   val keyedJsonStream = messageStream.keyBy(_.location)
      .window(TumblingEventTimeWindows.of(Time.minutes(5)))
     .aggregate(new AggregateFunction[Orders, Set[String], Long] {
      def createAccumulator(): Set[String] = Set.empty[String]
      def add( value: Orders, accumulator: Set[String]) = accumulator + value.seller_id
      def getResult(accumulator: Set[String]) = accumulator.size
      def merge(a:Set[String] , b:Set[String])= a ++ b
    })*/


    messageStream.print.setParallelism(1)
    env.execute("KafkaSource")
  }
}
