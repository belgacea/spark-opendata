package org.example.types

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

/** Helper class for Spark jobs
 * @group spark
 */
class SparkApp {

  /** Root Logger */
  @transient lazy val log: Logger = LogManager.getRootLogger
  /** Spark App class name */
  val appName: String = this.getClass.getSimpleName.filterNot("$".contains(_))
  /** Spark Session */
  @transient lazy val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .getOrCreate()
  /** Spark App start runtime */
  val startTimestamp: Long = System.nanoTime

  /**
   * Initiate Spark Job
   */
  def start(): Unit = {
    log.info(s"Starting $appName Spark Job at $startTimestamp")
  }

  /**
   * Shutting down Spark Session & log execution time
   */
  def stop(): Unit = {
    val duration = (System.nanoTime - startTimestamp) / 1e9d // 1e9d = 10^9 as a double to convert nano seconds to seconds
    log.info(s"Successfully run $appName in $duration seconds.")
    spark.stop()
  }

}
