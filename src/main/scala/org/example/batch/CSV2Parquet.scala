package org.example.batch

import org.example.types.{SparkApp, erpSchema}
import org.apache.spark.sql.DataFrame

/**
 * Read ERP dataset from CSV & rewrite it in Parquet (partition by 'type')
 * @author belgacea
 */
object CSV2Parquet extends SparkApp {

  @transient lazy val toolbox = BatchToolBox(spark)

  def main(args: Array[String]): Unit = {
    start()

    // Read CSV dataset
    val dataFrame : DataFrame = toolbox.loadCSV(args(0), Some(erpSchema))

    // Persist dataset in parquet format with a partition by 'type'
    toolbox.writeToParquetBy(dataFrame, args(1), Array("type"))

    stop()
  }

}