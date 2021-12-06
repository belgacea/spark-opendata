package org.example

import org.apache.log4j.{LogManager, Logger}

import java.net.URL
import scala.io.Source

/**
 * Spark utils
 *
 * @author belgacea
 * @group config
 */
package object utils {

  /** Root Logger */
  lazy val log: Logger = LogManager.getRootLogger

  /**
   * Parse property file
   *
   * @param filePath Path to file
   * @return Properties
   */
  def getConfig(filePath: String): Array[(String, String)] = {
    val file = Source.fromFile(filePath)
    try {
      file.getLines()
        .filter(line => line.contains("=") && !line.startsWith("#"))
        .map { line =>
          val tokens = line.split("=")
          (tokens(0), tokens(1))
        }.toArray
    } catch {
      case e: Exception => log.warn(e)
        Array()
    } finally {
      file.close()
    }
  }

  /** Get resources from local paths */
  def getResources(localPaths: List[String]): List[URL] = {
    localPaths.map(getClass.getResource)
  }

  /** Read configuration resources */
  def readConfigurationResources(configResources: List[URL]): List[(String, String)] = {
    configResources.map(_.getPath).flatMap(getConfig)
  }

}
