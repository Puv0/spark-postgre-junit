package common

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkCommon {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  def createSparkSession(): SparkSession = {
       val spark = SparkSession
         .builder()
         .appName("Spark Jobs")
         .master("local")
         .enableHiveSupport()
         .getOrCreate()
      logger.warn("Spark session started")

      spark
    }


}
