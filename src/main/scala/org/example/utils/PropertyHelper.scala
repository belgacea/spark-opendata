package org.example.utils

/**
 * Configuration property helper
 * @author belgacea
 * @groupname config Configuration
 */
trait PropertyHelper {

  /** Spark execution plan debug property */
  protected val explainExecutionPlanProperty: String = "spark.example.debug.execution.plan"
  /** Spark cache/persistence property */
  protected val enableCachePersistenceProperty: String = "spark.example.persistence.cache"
  /** SSpark persistence storage level property */
  protected val persistenceStorageLevelProperty: String = "spark.example.persistence.storage.level"

  /** CSV delimiter property */
  lazy protected val csvDelimiterProperty: String = "spark.example.csv.delimiter"
  /** CSV charset encoding property */
  lazy protected val csvCharsetEncodingProperty: String = "spark.example.csv.charset"

  /** Mandatory non-default properties */
  def properties: Array[String] = Array()

}
