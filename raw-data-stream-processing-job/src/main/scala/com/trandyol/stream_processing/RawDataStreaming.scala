package com.trandyol.stream_processing


object RawDataStreaming {


  def main(args: Array[String]): Unit = {

    val job = new RawDataStreamingSparkJob()
    job.process()
  }
}
