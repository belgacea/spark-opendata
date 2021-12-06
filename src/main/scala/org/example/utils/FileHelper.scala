package org.example.utils

import java.net.URLDecoder

/**
 * File helper functions
 *
 * @author belgacea
 */
trait FileHelper extends ConfigurationHelper {

  /** Filter unsafe URI character from an encoded string */
  def filterURIUnsafeCharacter(string: String): String = {
    val allowedCharacter = "[A-Za-z0-9._~-]"
    URLDecoder
      .decode(string, "ASCII")
      .filter(_.toString.matches(allowedCharacter))
  }

}
