package org.example.types

import org.example.DataFrameSuite
import org.scalatest.{BeforeAndAfter, FunSpec}

object SparkAppInstance extends SparkApp

class SparkAppSpec extends FunSpec with DataFrameSuite with BeforeAndAfter {

  var sparkApp: SparkApp = _

  before {
    sparkApp = SparkAppInstance
  }

  describe(testName) {

    it("should run spark") {
      sparkApp.start()
      assert(sparkApp.spark.emptyDataFrame.count() === 0)
      sparkApp.stop()
    }

  }
}