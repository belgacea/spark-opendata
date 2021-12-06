package org.example.batch

import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.example.DataFrameSuite
import org.example.types.erpSchema
import org.scalatest.{BeforeAndAfter, FunSpec}

/**
 * @author belgacea
 */
class BatchToolBoxSpec extends FunSpec with DataFrameSuite with BeforeAndAfter {

  import spark.implicits._
  var toolbox: BatchToolBox = _
  var inputDataframe: DataFrame = _

  before {
    toolbox = BatchToolBox(spark)
    val input = getResourceAbsolutePath("/input/bor_erp.csv")
    val inputSchema = erpSchema
    inputDataframe = csvToDataFrame(input, Some(inputSchema), Some(";"))
  }

  describe(testName) {

    describe(functionName("maxVisitorByStreet")){
      it("should process max visitor by street (with accommodation") {
        val expected = getResourceAbsolutePath(s"/test/accommodation/part-00000-471b05c8-33cb-4cdb-a594-75a99d778f81-c000.csv")
        val expectedSchema = StructType(Array(StructField("street", StringType), StructField("max_visitors", LongType)))
        val expectedDataframe = csvToDataFrame(expected, Some(expectedSchema))
        val outputDataframe = toolbox.maxVisitorByStreet(inputDataframe)
          .select(functions.trim(col("street")).as("street"), col("max_visitors")) // Working around smelly data with smelly code (ง ͡ʘ ل͟ ͡ʘ)ง
        assertSmallDatasetEquality(outputDataframe, expectedDataframe, orderedComparison = false)
      }
      it("should process max visitor by street (without accommodation)") {
        val expected = getResourceAbsolutePath(s"/test/no_accommodation/part-00000-cefcd4be-2690-426b-b170-ea042ec669ac-c000.csv")
        val expectedSchema = StructType(Array(StructField("street", StringType), StructField("max_visitors", LongType)))
        val expectedDataframe = csvToDataFrame(expected, Some(expectedSchema))
        val outputDataframe = toolbox.maxVisitorByStreet(inputDataframe, accommodation = false)
        assertSmallDatasetEquality(outputDataframe, expectedDataframe)
      }
    }

    describe(functionName("getUnknownStreet")){
      it("should retrieve unhandled street names") {
        val expected = getResourceAbsolutePath(s"/test/unknown/part-00000-dcbe2a9a-fbc5-4ed6-b571-adfcda400a77-c000.csv")
        val expectedSchema = StructType(Array(
          StructField("nom_etablissement", StringType),
          StructField("adresse_1", StringType),
          StructField("adresse_2", StringType),
          StructField("nb_visiteurs_max", LongType)
        ))
        val expectedDataframe = csvToDataFrame(expected, Some(expectedSchema))
        val outputDataframe = toolbox.getUnknownStreet(inputDataframe)
//        writeToCSVOutput(outputDataframe, "./output/unknown")
        assertSmallDatasetEquality(outputDataframe, expectedDataframe)
      }
    }

  }

}