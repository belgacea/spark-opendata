package org.example.utils

import org.apache.spark.sql.SparkSession
import org.example.DataFrameSuite
import org.scalatest.{BeforeAndAfter, FunSpec}

/**
 * @author belgacea
 */
class ConfigurationHelperSpec extends FunSpec with DataFrameSuite with BeforeAndAfter {

  var configuration: ConfigurationHelper = _

  before {
    configuration = ConfigurationHelperInstance(spark)
  }

  describe(testName) {

    def testProperty(prop: String): Unit = {
      it(s"should have a '$prop' property") {
        assert(configuration.getConfigProp(prop).nonEmpty)
      }
    }
    PropertyHelperInstance.properties.foreach( property => testProperty(property))

    it("should have a val explainExecutionPlan: Boolean") {
      assert(configuration.explainExecutionPlan.isInstanceOf[Boolean])
    }

    it("should fail to get empty property") {
      assertThrows[IllegalArgumentException] {
        val configProp = configuration.getConfigProp("")
      }
    }
    it("should fail to get empty property when a default value is provided and not empty") {
      assertThrows[IllegalArgumentException] {
        val configProp = configuration.getConfigProp("", "default")
      }
    }
    it("should fail to get a missing property array") {
      assertThrows[NoSuchElementException] {
        val configProp = configuration.getConfigPropArray("spark.unknown.property")
      }
    }
    it("should get a missing property array when a default value is provided and not empty") {
      val configProp = configuration.getConfigPropArray("spark.unknown.property", Array("default"))
      assert(configProp, Array("default"))
    }
  }
}

case class ConfigurationHelperInstance(spark: SparkSession) extends ConfigurationHelper