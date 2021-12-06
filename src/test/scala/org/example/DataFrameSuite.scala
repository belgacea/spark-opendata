package org.example

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.scalatest.Suite
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.example.utils.{CSVHelper, TestHelper}

/**
 * [[DataFrame]] suite for testing
 * @author belgacea
 */
trait DataFrameSuite extends SparkSuiteBase
  with DatasetComparer
  with TestHelper
  with CSVHelper {
  self: Suite =>

  @transient override lazy val context: SparkContext = spark.sparkContext

  /**
   * Get resource absolute path from relative path
   * @param relativePath Relative path to resource file
   * @return Absolute path to resource file
   */
  def getResourceAbsolutePath(relativePath: String): String = getClass.getResource(relativePath).getPath

  /**
   * Load csv file in a Spark [[DataFrame]]
   * @param path Path to csv file
   * @param schema Optional schema
   * @return
   */
  def csvToDataFrame(path: String, schema: Option[StructType], delimiter: Option[String] = Some(",")): DataFrame = {
    loadCSV(path, schema, delimiter)
  }

  /**
   * Write [[DataFrame]] to local output folder
   * @param dataframe Output [[DataFrame]]
   */
  def writeToCSVOutput(dataframe: DataFrame, path: String = "./output/"): Unit = {
    writeToCSV(dataframe, path)
  }

}
