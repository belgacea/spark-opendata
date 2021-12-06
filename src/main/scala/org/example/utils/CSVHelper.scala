package org.example.utils

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * Comma Separated Values helper functions for batch processing
 *
 * @group csv
 * @author belgacea
 */
trait CSVHelper extends FileHelper {

  /**
   * Read CSV file using a specified schema
   *
   * @group csv
   * @param path      HDFS path of the csv file
   * @param schema    Schema of the csv file
   * @param delimiter Delimiter option (Default : ",")
   * @param header    Header option (Default : "true")
   * @return Csv file as [[DataFrame]]
   */
  def loadCSV(path: String,
              schema: Option[StructType] = None,
              delimiter: Option[String] = None,
              header: Boolean = true,
              timestampFormat: String = "yyyy/MM/dd HH:mm:ss"): DataFrame = {
    val separator = delimiter match {
      case Some(d) => d
      case None => csvDelimiter
    }
    val readCSV = spark.read.format("csv")
      .option("sep", separator)
      .option("header", header)
      .option("charset", "UTF8")
      .option("timestampFormat", timestampFormat)
    val dataFrameReader = schema match {
      case Some(s) => readCSV.schema(s)
      case None => readCSV.option("inferSchema", value = true) // Not available if headers contains quotes
    }
    dataFrameReader.load(path)
  }

  /**
   * Write [[DataFrame]] to one CSV file
   *
   * //    * @note WARNING : This function uses 1 partition to write a single CSV file.
   * //    *       It may have a serious impact on the overall efficiency of the application from which it is called.
   *
   * @note When HDFS extraction is enabled, headers are disabled and added after the transfer
   * @group csv
   * @param dataframe [[DataFrame]] to write
   * @param path      Output file path
   * @param header    Whether to write headers or not in csv output ( Default : true )
   */ // TODO custom delimiter ?
  def writeToCSV(dataframe: DataFrame, path: String,
                 partitionBy: Option[Array[String]] = None,
                 header: Boolean = true): Unit = {
    require(path.nonEmpty, "Undefined output path. Can't write csv file if path is empty.")
    if (explainExecutionPlan) dataframe.explain(extended = true)
    val output = partitionBy match {
        case Some(partitions) => dataframe
          .repartition(partitions.map(col): _*)
          .write
          .partitionBy(partitions: _*)
        case None => dataframe
          .coalesce(1)
          .write
    }
    output
      .mode(SaveMode.Overwrite)
      .option("header", header.toString)
      .option("charset", csvCharsetEncoding)
      .csv(path)
  }

}
