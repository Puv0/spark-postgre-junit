import common.{PostgreCommon, SparkCommon}
import models.{Customer, LegalCustomer}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.slf4j.LoggerFactory
object CheckCustomerAgeJob {

  private val Logger = LoggerFactory.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
      Logger.info("Main method started")
      val spark = SparkCommon.createSparkSession()

      spark.sparkContext.setLogLevel("Error")

      import spark.implicits._

      val allCustomer = PostgreCommon.readFromPostgreTable("customer",spark)
        .as[Customer]


        val legalCustomer = checkCustomerIsLegal(allCustomer,spark)

        legalCustomer.show()
       PostgreCommon.writeToPostgreTable(legalCustomer,"isCustomerLegal")

    }
  def checkCustomerIsLegal(customer:Dataset[Customer],spark:SparkSession): Dataset[LegalCustomer] = {
    import spark.implicits._

    customer
      .map{ c =>
      if(c.age >= 18) LegalCustomer(c.customer_id,c.cname, c.age, true)
      else   LegalCustomer(c.customer_id,c.cname, c.age, false)
      }
  }


}
