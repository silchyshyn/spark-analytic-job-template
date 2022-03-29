package com.trendyol.raw_data_processing

import com.trendyol.raw_data_processing.config.StructuringConfig
import com.typesafe.config.ConfigFactory
import com.trendyol.raw_data_processing.job.RawDataProcessingJob
import org.apache.spark.sql.SparkSession

import java.io.File

object RawDataProcessing {
  val appName: String = this.getClass.getSimpleName.replace("$", "")

  case class JobArgs(configFile: Option[String] = None,
                     local: Boolean = false,
                     ordersSourceFile: String = null,
                     productSourceFile: String = null,
                     commonConfigFile: String = null)

  private val parser = new scopt.OptionParser[JobArgs](appName) {
    head(appName)
    opt[String]('f', "config-file")
      .optional()
      .action((f, c) => c.copy(configFile = Some(f)))
      .valueName("<config-file>")
      .text("Job configuration file")
    opt[String]("orders-source-file")
      .required()
      .action((ordersSourceFile, c) => c.copy(ordersSourceFile = ordersSourceFile))
      .valueName("<sourceFilePath>")
      .text("The path to file containing orders data")
    opt[String]("products-source-file")
      .required()
      .action((productSourceFile, c) => c.copy(productSourceFile = productSourceFile))
      .valueName("<sourceFilePath>")
      .text("The path to file containing products data")
    opt[Unit]('l', "local-mode")
      .optional()
      .action((_, c) => c.copy(local = true))
      .valueName("<local-mode>")
      .text("Set to run with local[*] master")
    opt[String]('f', "common-config-file")
      .required()
      .action((f, c) => c.copy(commonConfigFile = f))
      .valueName("<common-config-file>")
      .text("Job common configuration file")
  }

  def main(args: Array[String]): Unit = parser.parse(args, JobArgs()) match {
    case Some(arguments) =>
      val appName = s"raw-data-processing"
      val config = arguments.configFile
        .map(new File(_))
        .map(ConfigFactory.parseFile)
        .getOrElse(ConfigFactory.empty())
      val commonConfig = new File(arguments.commonConfigFile)
      val rawAppConfig = StructuringConfig.from(ConfigFactory.parseFile(commonConfig), config)
      val spark = if (arguments.local) {
        SparkSession.builder()
          .appName(appName)
          .master("local[*]")
          .getOrCreate()
      } else {
        SparkSession.builder()
          .appName(appName)
          .getOrCreate()
      }

      val job = new RawDataProcessingJob(
        rawAppConfig,
        arguments.ordersSourceFile,
        arguments.productSourceFile,
        spark)
      job.process()
    case _ => sys.exit(-1)
  }
}

