package org.example.utils

import org.apache.spark.SparkContext

/**
 * Helper trait used to retrieve Spark configuration properties
 *
 * @author belgacea
 * @note Keep it lazy pls ( ͡° ͜ʖ ͡°)
 * @group config
 * @groupname csv CSV
 * @define configDefinition defined in configuration as :
 */
trait ConfigurationHelper extends SessionHandler
  with PropertyHelper {

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Spark options
  //////////////////////////////////////////////////////////////////////////////////////////////

  /** Spark execution plan debug option $configDefinition "spark.example.debug.execution.plan"
   *
   * @note Default : false
   * @group spark */
  lazy val explainExecutionPlan: Boolean = getConfigProp(explainExecutionPlanProperty, "false").toBoolean
  /** Spark cache/persistence option $configDefinition "spark.example.persistence.cache"
   *
   * @note Default : true
   * @group spark */
  lazy val enableCachePersistence: Boolean = getConfigProp(enableCachePersistenceProperty, "true").toBoolean
  /** Spark persistence storage level $configDefinition "spark.example.persistence.storage.level"
   *
   * @note Default : MEMORY_AND_DISK_SER
   * @group spark */
  lazy val persistenceStorageLevel: String = getConfigProp(persistenceStorageLevelProperty, "MEMORY_AND_DISK_SER")

  /** CSV delimiter $configDefinition "spark.example.csv.delimiter"
   *
   * @note Default : ","
   * @group csv */
  lazy val csvDelimiter: String = getConfigProp(csvDelimiterProperty, ",")
  /** CSV charset encoding $configDefinition "spark.example.csv.charset"
   *
   * @note Default : "utf-8"
   * @group csv */
  lazy val csvCharsetEncoding: String = getConfigProp(csvCharsetEncodingProperty, "utf-8")


  //////////////////////////////////////////////////////////////////////////////////////////////
  // Functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Get context configuration properties from [[SparkContext]] as [[String]]
   *
   * @group spark
   * @param name Name of the property to retrieve
   * @return Property value as [[String]]
   */
  def getConfigProp(name: String): String = {
    require(name.nonEmpty, "Property name could not be empty !")
    context.getConf.get(name)
  }

  /**
   * Get context configuration properties from [[SparkContext]] as [[String]]
   *
   * @group spark
   * @param name    Name of the property to retrieve
   * @param default Optional default value
   * @return Property value as [[String]]
   */
  def getConfigProp(name: String, default: String): String = {
    require(name.nonEmpty, "Property name could not be empty !")
    context.getConf.get(name, default)
  }

  /**
   * Get context configuration properties from [[SparkContext]]
   *
   * @group spark
   * @param name    Name of the property to retrieve
   * @param default Optional default value
   * @return Property value
   */
  def getConfigPropArray(name: String, default: Array[String] = Array()): Array[String] = {
    try {
      getConfigProp(name).split(",")
    } catch {
      case e: NoSuchElementException => if (default.nonEmpty) default else throw e
    }
  }

}
