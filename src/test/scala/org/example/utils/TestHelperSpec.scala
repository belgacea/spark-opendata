package org.example.utils

import org.scalatest.{BeforeAndAfter, FunSpec}

/**
 * @author belgacea
 */
class TestHelperSpec extends FunSpec with BeforeAndAfter {

  var test: TestHelper = TestHelperInstance

  describe("TestHelperSpec") {

    describe("testName"){
      it("should get class name") {
        assert(test.testName === "TestHelperInstance")
      }
    }
    describe("functionName"){
      it("should suffix function names") {
        assert(test.functionName("myFunction") === "myFunction function")
      }
    }

  }
}

object TestHelperInstance extends TestHelper