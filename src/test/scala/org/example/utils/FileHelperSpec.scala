package org.example.utils

import org.apache.spark.sql.SparkSession
import org.example.DataFrameSuite
import org.scalatest.{BeforeAndAfter, FunSpec}

/**
 * @author belgacea
 */
class FileHelperSpec extends FunSpec with DataFrameSuite with BeforeAndAfter {

  var file: FileHelper = _

  before {
    file = FileHelperInstance(spark)
  }

  describe(testName) {

    describe(functionName("filterURIUnsafeCharacter")){
      it("should filter URI unsafe character in string") {
        val input = "B4%3AE6%3A2D%3A53%3A99%3AF6"
        val output = file.filterURIUnsafeCharacter(input)
        val expected = "B4E62D5399F6"
        assert(output === expected)
      }
    }

  }
}

case class FileHelperInstance(spark: SparkSession) extends FileHelper