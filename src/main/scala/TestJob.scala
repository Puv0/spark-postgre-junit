import common.{PostgreCommon, SparkCommon}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory
object TestJob {

  private val Logger = LoggerFactory.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
      Logger.info("Main method started")
      val spark = SparkCommon.createSparkSession()

      spark.sparkContext.setLogLevel("Error")

      import spark.implicits._

      //val df =   spark.read.jdbc(postgresConfig.getUrl(),postgresConfig.getTable("test"),postgresConfig.getProperties())
      //df.show()
      // df.write.format("json").save("sample")
      val df = PostgreCommon.readFromPostgreTable("test",spark)

      PostgreCommon.writeToPostgreTable(df,"test_to_write")

    }
}
