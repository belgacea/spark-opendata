package org.example.batch

import org.apache.spark.sql.DataFrame
import org.example.types.{SparkApp, erpSchema}

/**
 * Process maximum capacity of visitors per street (with or without accommodation)
 * @author belgacea
 */
object ERPMaxVisitorByStreet2CSV extends SparkApp {

  @transient lazy val toolbox = BatchToolBox(spark)

  def main(args: Array[String]): Unit = {
    start()

    // Read csv dataset
    val dataFrame : DataFrame = toolbox.loadCSV(args(0), Some(erpSchema))

    // Calculate the maximum capacity of visitors per street, with or without accommodation
    val maxVisitorByStr = toolbox.maxVisitorByStreet(dataFrame, accommodation = args(1).toBoolean)

    // Write results to CSV file
    toolbox.writeToCSV(maxVisitorByStr, args(2))

    stop()
  }

}