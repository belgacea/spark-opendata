package org.example.utils

import org.example.DataFrameSuite
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSpec}

/**
 * @author belgacea
 */
class SessionHandlerSpec extends FunSpec with DataFrameSuite with BeforeAndAfter {

  var sessionHandler: SessionHandler = _

  before {
    sessionHandler = SessionHandlerInstance(spark)
  }

  describe("SessionHandler") {
    it("should have a spark") {
      assert(sessionHandler.spark !== null)
    }
    it("should have a context") {
      assert(sessionHandler.context !== null)
    }
    it("should have a sql context") {
      assert(sessionHandler.sql !== null)
    }
  }

}

case class SessionHandlerInstance(spark: SparkSession) extends SessionHandler