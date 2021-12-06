package org.example

import org.apache.spark.sql.types._

/**
 * Data types
 *
 * @author belgacea
 */
package object types {

  /**
   * ERP Data Schema
   */
  val erpSchema: StructType = StructType(
    Array(
      StructField("avec_hebergement", StringType, true),
      StructField("nom_etablissement", StringType, true),
      StructField("propriete_ville", StringType, true),
      StructField("categorie", IntegerType, true),
      StructField("type", StringType, true),
      StructField("adresse_1", StringType, true),
      StructField("adresse_2", StringType, true),
      StructField("code_postal", StringType, true),
      StructField("commune", StringType, true),
      StructField("canton", StringType, true),
      StructField("effectif_personnel", LongType, true),
      StructField("nb_visiteurs_max", LongType, true),
      StructField("geometrie", StringType, true),
    )
  )
}
