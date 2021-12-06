package org.example

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.example.utils.{CSVHelper, ParquetHelper}

/**
 * Spark batch package
 * @author belgacea
 * @groupname batch Batch
 * @groupname aggregation Aggregation
 */
package object batch {

  /**
   * Batch toolbox for Spark helper functions
   *
   * @param spark [[SparkSession]] used to execute helper functions
   */
  case class BatchToolBox(spark: SparkSession) extends Serializable
    with CSVHelper
    with ParquetHelper {

    /** Street Regex */
    val streetRegex = "(?:0-9*[ ])*((all)|(alle)|(allée)|(allee)|(allées)|(allees)|(alléee)|(av)|(ave)|(avenue)|(avenus)|(bassin)|(bassins)|(batiment)|(bâtiment)|(bd)|(bld)|(bdl)|(boulevard)|(carrefour)|(caserne)|(clairiere)|(centre)|(chemin)|(chaussée)|(chaussee)|(cité)|(cite)|(corniche)|(crs)|(cours)|(domaine)|(descente)|(ecart)|(espace)|(esplanade)|(faubourg)|(flèche)|(fleche)|(gare)|(grande)|(gymnase)|(hameau)|(halle)|(halles)|(hémicycle)|(hemicycle)|(hangar)|(hotel)|(impasse)|(ilot)|(jardin)|(jardins)|(lieu\\-dit)|(lotissement)|(mail)|(marché)|(marche)|(mairie)|(montée)|(montee)|(niveau)|(niveaux)|(palais)|(passage)|(parc)|(pl)|(place)|(placette)|(plaine)|(plateau)|(pont)|(port)|(promenade)|(parvis)|(quartier)|(quai)|(quais)|(résidence)|(residence)|(ruelle)|(rocade)|(rond\\-point)|(route)|(rue)|(rues)|(sentier)|(site)|(square)|(terrasse)|(stade)|(terre\\-plein)|(terre plein)|(traverse)|(tour)|(villa)|(village)|(zac))([ ]+[\\w '-]*)"

    protected val (address1Col, address2Col) = ("adresse_1", "adresse_2")
    protected val (street, streetCol1, streetCol2, accommodationCol) = ("street", "street1", "street2", "avec_hebergement")
    protected val streetCol1Epxr: Column = regexp_extract(lower(col(address1Col)), streetRegex, 0)
    protected val streetCol2Epxr: Column = regexp_extract(lower(col(address2Col)), streetRegex, 0)
    protected val streetColEpxr: Column = getStreetColExpr(col(streetCol1), col(streetCol2))

    /**
     * Street column fallback expression
     *
     * @param column1 Street column 1
     * @param column2 Street column 2
     * @return Street column
     */
    def getStreetColExpr(column1: Column, column2: Column): Column = {
      when(column1.isNotNull and !(column1 <=> lit("")),
        column1)
        .otherwise(
          when(column2.isNotNull and !(column2 <=> lit("")),
            column2)
            // .otherwise(lit("Unknown"))
            .otherwise(
              when(col(address1Col).isNotNull and !(col(address1Col) <=> lit("")),
                col(address1Col))
                .otherwise(
                  lit("Unknown"))
            )
        )
    }

    /**
     * Get max visitor by street (filtered by accommodation)
     *
     * @group aggregation
     * @param dataFrame Input dataframe
     * @param accommodation Accommodation filter value
     */
    def maxVisitorByStreet(dataFrame: DataFrame, accommodation: Boolean = true): DataFrame = {
      val accommodationValue = if (accommodation) "0" else "N"
      dataFrame
        .filter(col(accommodationCol) <=> lit(accommodationValue)) // Filter by accommodation column
        .withColumn(streetCol1, streetCol1Epxr)
        .withColumn(streetCol2, streetCol2Epxr)
        .withColumn(street, streetColEpxr) // Consolidate street column
        .groupBy(street)
        .agg(sum(col("nb_visiteurs_max")).as("max_visitors")) // Process max visitor by street
    }

    /**
     * Get unmatched streets
     * @deprecated
     * @note 29k visitors seats hidden somewhere ¯\_(ツ)_/¯
     * but that's without mentioning the super massive black hole
     * which collapsed in rue jean samazeuihl
     *
     * @param dataFrame Input dataframe
     */
    def getUnknownStreet(dataFrame: DataFrame): DataFrame = {
      dataFrame
        .withColumn(streetCol1, streetCol1Epxr)
        .withColumn(streetCol2, streetCol2Epxr)
        .withColumn(street, streetColEpxr) // Consolidate street column
        .filter(col(street) <=> lit("Unknown"))
//        .filter(col(streetCol1).isNotNull)
        .select(
          col("nom_etablissement"),
          col("adresse_1"),
          col("adresse_2"),
          col("nb_visiteurs_max"),
        )

    }

  }

}
