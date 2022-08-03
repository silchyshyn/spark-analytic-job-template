package com.trandyol.stream_processing

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.json4s.{DefaultFormats, JValue}
import org.json4s.jackson.JsonMethods

case class Order(
                  customer_id: String,
                  location: String,
                  order_date: String,
                  order_id: String,
                  price: Double,
                  seller_id: String,
                  status: String)

class RawDataStreamingJob extends Serializable with LazyLogging{

  implicit val typeInfo: TypeInformation[String] = TypeInformation.of(classOf[String])
  implicit lazy val jValue: TypeInformation[JValue] = TypeInformation.of(classOf[JValue])
  implicit  val typeInfoOrder: TypeInformation[Order] = TypeInformation.of(classOf[Order])
  implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats

  def process(): Unit = {

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

    val keyedJsonStream = messageStream.keyBy(_.location)
      .window(TumblingEventTimeWindows.of(Time.minutes(5)))
      .aggregate(new AggregateFunction[Order, Set[String], Long] {
        def createAccumulator() = Set.empty[String]
        def add( value: Order, accumulator: Set[String]): Set[String] = accumulator + value.seller_id
        def getResult(accumulator: Set[String]): Long = accumulator.size
        def merge(a:Set[String] , b:Set[String]): Set[String] = a ++ b
      })

    messageStream.print.setParallelism(1)
    env.execute("KafkaSource")
  }

}
