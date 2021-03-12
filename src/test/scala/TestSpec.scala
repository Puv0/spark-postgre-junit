import common.SparkCommon
import models.{Customer, LegalCustomer}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

class TestSpec extends AnyFunSuite with BeforeAndAfterEach  {

  var spark: SparkSession = _

  override def beforeEach(): Unit = {

    spark = SparkCommon.createSparkSession()

  }
  test("should add new column to dataset") {

    val sparkimplicit = spark.sqlContext
    import sparkimplicit.implicits._
    val sourceDS = Seq(Customer("1","Cameron Mays", 16),
      Customer("2","Abbot Duke", 27),
      Customer("3","Price Dotson", 15),
      Customer("4","Silas Bright", 41)
    ).toDS()

    val expectedResult = Seq(
      LegalCustomer("1","Cameron Mays",16, false),
      LegalCustomer("2","Abbot Duke",27, true),
      LegalCustomer("3","Price Dotson",15, false),
      LegalCustomer("4","Silas Bright",41, true)
    ).toDS().collectAsList()

    val actualResult = CheckCustomerAgeJob.checkCustomerIsLegal(sourceDS, spark)
      .collectAsList()
      assert(actualResult == expectedResult)

  }
  test("is 18 age should be legal? "){
    val sparkimplicit = spark.sqlContext
    import sparkimplicit.implicits._


    val sourceDS = Seq(Customer("57","Aladdin Cannon", 18)).toDS()

    val expectedResult = Seq(
      LegalCustomer("57","Aladdin Cannon", 18,true),
    ).toDS().collectAsList()

    val actualResult = CheckCustomerAgeJob.checkCustomerIsLegal(sourceDS,spark)
      .collectAsList()

    assert(actualResult == expectedResult)
  }

  override def afterEach(): Unit = {
    spark.stop()
  }


}
