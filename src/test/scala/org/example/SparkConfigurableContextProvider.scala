package org.example

import com.holdenkarau.spark.testing.SparkContextProvider
import org.apache.spark.SparkConf
import org.example.utils.{getResources, readConfigurationResources}

import java.net.URL

/**
 * Spark context provider used to setup custom properties from configuration files
 *
 * @author belgacea
 * @note Source : http://fruzenshtein.com/scala-working-with-resources-folders-files/
 */
trait SparkConfigurableContextProvider extends SparkContextProvider {

  private val resources = "/config/config.dev.properties" :: Nil

  override def conf: SparkConf = {
    // Retrieve custom configuration
    val customConfigResources: List[URL] = getResources(resources)
    // Read custom config
    val customConfig: List[(String, String)] = readConfigurationResources(customConfigResources)
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("test")
      .set("spark.ui.enabled", "false")
      .set("spark.app.id", appID)
      .set("spark.driver.host", "localhost")
      // Force UTC tz
      .set("spark.sql.session.timeZone", "UTC")
      .set("user.timezone", "UTC")
      .set("spark.sql.shuffle.partitions", "1")
      // Load custom config inside spark context
      .setAll(customConfig)
  }

}
