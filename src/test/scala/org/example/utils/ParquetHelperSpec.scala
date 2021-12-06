package org.example.utils

import org.apache.spark.sql.SparkSession
import org.example.DataFrameSuite
import org.example.types.erpSchema
import org.scalatest.{BeforeAndAfter, FunSpec}

import java.nio.file.{Files, Paths}

/**
 * @author belgacea
 */
class ParquetHelperSpec extends FunSpec with DataFrameSuite with BeforeAndAfter {

  var parquet: ParquetHelper = _

  before {
    parquet = ParquetHelperInstance(spark)
  }

  describe(testName) {

    it("should write dataframes to parquet file format (partition by type)") {
      val input = getResourceAbsolutePath("/input/bor_erp.csv")
      val inputSchema = erpSchema
      val inputDataframe = csvToDataFrame(input, Some(inputSchema), Some(";"))
      val path = "./output/bor_erp"
      parquet.writeToParquetBy(inputDataframe, path, Array("type"))
      assert(Files.exists(Paths.get(path)))
    }

  }
}

case class ParquetHelperInstance(spark: SparkSession) extends ParquetHelper