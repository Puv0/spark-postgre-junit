package common

import models.LegalCustomer
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.util.Properties

object PostgreCommon{

  private val logger = LoggerFactory.getLogger(getClass.getName)


  def getTable(tableName:String): String = {
    val pgTable = tableName
    logger.warn("getTable")
    pgTable
  }

  def getPostgreProperties(): Properties ={
    val pgConnectionProperties = new Properties()
    pgConnectionProperties.put("user","postgres")
    pgConnectionProperties.put("password","postgres")
    logger.warn("GetPostgreProperties")
    pgConnectionProperties
  }

  def getPostgreUrl():String = {
    val url = "jdbc:postgresql://localhost:5432/Test"
    logger.warn("GetPostgreURL")
    url
  }

  def readFromPostgreTable(tableName:String, spark:SparkSession): DataFrame = {
    val df = spark.read.jdbc(getPostgreUrl(),getTable(tableName),getPostgreProperties())
    df
  }
  def writeToPostgreTable(df:Dataset[LegalCustomer], tableName: String):Unit = {
    val user = getPostgreProperties().getProperty("user")
    val password = getPostgreProperties().getProperty("password")
    logger.warn("Writing session started")
   try {

    df.write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url",getPostgreUrl())
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
