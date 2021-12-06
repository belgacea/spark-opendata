package org.example

import com.holdenkarau.spark.testing.{DataFrameSuiteBaseLike, SharedSparkContext, TestSuite, TestSuiteLike}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.Suite

/**
 * :: Experimental ::
 * Base class for testing Spark DataFrames
 * @note Sources :
 *       https://github.com/holdenk/spark-testing-base/blob/master/core/src/main/2.0/scala/com/holdenkarau/spark/testing/DataFrameSuiteBase.scala
 *       https://github.com/holdenk/spark-testing-base/issues/186
 */
trait SparkSuiteBase extends TestSuite
  with SharedSparkContext
  with SparkSuiteBaseLike { self: Suite =>

  var config: SparkConf = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    super.sqlBeforeAllTestCases()
    config = context.getConf
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (!reuseContextIfPossible) {
      SparkSessionProvider._sparkSession = null
    }
  }

}

trait SparkSuiteBaseLike extends SparkConfigurableContextProvider
  with TestSuiteLike with DataFrameSuiteBaseLike with Serializable {

  @transient lazy val context: SparkContext = spark.sparkContext

}

object SparkSessionProvider {

  @transient var _sparkSession: SparkSession = _

  def sparkSession: SparkSession = _sparkSession

  def context: SparkContext = _sparkSession.sparkContext

  def sqlContext: SQLContext = _sparkSession.sqlContext
  //  def sqlContext = EvilSessionTools.extractSQLContext(_sparkSession) // Get rid of EvilSessionTools

}