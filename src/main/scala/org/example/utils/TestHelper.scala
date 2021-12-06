package org.example.utils

/**
 * Test Helper trait
 * @group test
 * @author belgacea
 */
trait TestHelper {

  /** Test Spec Class name */
  val testName: String = this.getClass.getSimpleName.filterNot("$".contains(_))

  /** Suffix function names */
  def functionName(name: String): String = s"${name} function"

}
