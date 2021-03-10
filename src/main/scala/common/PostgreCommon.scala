package common

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.util.Properties

object PostgreCommon{

  private val logger = LoggerFactory.getLogger(getClass.getName)


  def getTable(tableName:String): String = {
    val pgTable = tableName
    logger.warn("getTable")
    pgTable
  }

  def getProperties(): Properties ={
    val pgConnectionProperties = new Properties()
    pgConnectionProperties.put("user","postgres")
    pgConnectionProperties.put("password","postgres")
    logger.warn("GetProperties")
    pgConnectionProperties
  }

  def getUrl():String = {
    val url = "jdbc:postgresql://localhost:5432/Test"
    logger.warn("GetURL")
    url
  }

  def readFromPostgreTable(tableName:String, spark:SparkSession): DataFrame = {
    val df = spark.read.jdbc(getUrl(),getTable(tableName),getProperties())
    df
  }
  def writeToPostgreTable(df:DataFrame,tableName: String):Unit = {
    val user = getProperties().getProperty("user")
    val password = getProperties().getProperty("password")
    logger.warn("Writing session started")
   try {

    df.write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url",getUrl())
      .option("dbtable",getTable(tableName))
      .option("user", user)
      .option("password",password)
      .save()

     logger.warn("WriteToPostgreEnded")

   }catch {
     case e : Exception =>
       logger.error("An error occured" + e.printStackTrace())
   }
  }
}
