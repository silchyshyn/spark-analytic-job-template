package com.trandyol.stream_processing

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._
import play.api.libs.json.{JsSuccess, Json}

import java.util.Properties

class RawDataStreamingJob {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  case class JsonMessage(product_id: String, seller_id: Double, location: String)
  implicit val jsonMessageReads = Json.reads[JsonMessage]


  /*val source = KafkaSource.builder()
    .setBootstrapServers("localhost:29092")
    .setTopics("orders")
    .setGroupId("orders-group")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build()

  val messageStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")

  val jsonStream = messageStream
    .map(entry => Json.fromJson[JsonMessage](Json.parse(entry)))
    .filter(_.isInstanceOf[JsSuccess[JsonMessage]])
    .map(_.get)*/

  //val keyedJson = filteredJsonStream.keyBy(_.id)
  //val keyedWindowed = keyedJson.timeWindow(Time.seconds(10))
}
