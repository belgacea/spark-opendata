package org.example.utils

import org.example.DataFrameSuite
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSpec}

/**
 * @author belgacea
 */
class PropertyHelperSpec extends FunSpec with DataFrameSuite with BeforeAndAfter {

  var propertyHelper: PropertyHelper = _

  before {
    propertyHelper = PropertyHelperInstance
  }

  describe("PropertyHelper") {

    it("should have mandatory non-default properties listed") {
      assert(propertyHelper.properties === Array()) // Empty for now
    }

  }

}

object PropertyHelperInstance extends PropertyHelper