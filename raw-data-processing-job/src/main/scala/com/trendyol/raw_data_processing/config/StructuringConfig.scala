package com.trendyol.raw_data_processing.config

import com.typesafe.config.Config
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.DataType
import pureconfig.{ConfigReader, ConfigSource}


case class StructuringConfig(//sourceDatasetSchemasLocation: String,
                             sourceDatasets: Seq[DatasetConf],
                             sourcePartitions: Int,
                             sourceFileFormat: String,
                             targetFileFormat: String,
                             targetProductsPath: String,
                             targetPath: String,
                             targetPriceBehaviorPath: String
                            )

case class DatasetConf
(
  dataset: String,
  datasetSchema: Map[String, DataType],
  //mapping: Map[String, String]
)

object StructuringConfig {

  def from(commonConfiguration: Config, configuration: Config): StructuringConfig = {
    implicit val sparkTypeReader: ConfigReader[DataType] = ConfigReader[String].map(CatalystSqlParser.parseDataType)
    import pureconfig.generic.auto._

    val config = ConfigSource.fromConfig(configuration)
    val commonConfig = ConfigSource.fromConfig(commonConfiguration)
    config.withFallback(commonConfig).loadOrThrow[StructuringConfig]
  }
}