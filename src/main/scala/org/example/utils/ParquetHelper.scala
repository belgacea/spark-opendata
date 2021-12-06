package org.example.utils

import org.apache.spark.sql.{Dataset, SaveMode}

/**
 * Parquet helper functions
 *
 * @group parquet
 * @author belgacea
 */
trait ParquetHelper extends FileHelper {

  def writeToParquetBy(dataframe: Dataset[_], path: String, partitions: Array[String]): Unit = {
    dataframe.write
      .mode(SaveMode.Overwrite)
      .partitionBy(partitions: _*)
      .parquet(path)
  }
}
