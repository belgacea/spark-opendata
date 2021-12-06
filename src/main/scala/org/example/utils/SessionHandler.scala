package org.example.utils

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
 * Spark Session & Context references trait handler
 * @author belgacea
 * @groupname spark Spark
 */
trait SessionHandler {

  /** Root Logger */
  @transient lazy val log: Logger = LogManager.getRootLogger

  /** Spark Session
   * @group spark */
  implicit def spark: SparkSession

  /** Spark Context
   * @group spark */
  @transient lazy val context: SparkContext = spark.sparkContext

  /** Spark SQL Context
   * @group spark */
  @transient lazy val sql: SQLContext = spark.sqlContext

}
