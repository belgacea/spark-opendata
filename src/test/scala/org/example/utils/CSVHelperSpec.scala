package org.example.utils

import org.apache.spark.sql.SparkSession
import org.example.DataFrameSuite
import org.scalatest.{BeforeAndAfter, FunSpec}

import java.nio.file.{Files, Paths}

/**
 * @author belgacea
 */
class CSVHelperSpec extends FunSpec with DataFrameSuite with BeforeAndAfter {

  import spark.implicits._
  var csv: CSVHelper = _

  before {
    csv = CSVHelperInstance(spark)
  }

  describe(testName) {

    it("should write csv file") {
      val path = "./output/test"
      writeToCSVOutput(spark.sparkContext.parallelize(Seq(("test", 1, 2.0))).toDF("id", "count", "sum"), path)
      assert(Files.exists(Paths.get(path)))
    }

  }
}

case class CSVHelperInstance(spark: SparkSession) extends CSVHelper