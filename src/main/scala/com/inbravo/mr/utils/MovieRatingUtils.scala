package com.inbravo.mr.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.recommendation.Rating
import com.inbravo.mr.config.ProjectConfig

/**
 * amit.dixit
 *
 * Movie ratings related functions
 */
object MovieRatingUtils extends ProjectConfig {

  /**
   *  Get RDD of all ratings
   */
  def ratingsRDD(sparkSession: SparkSession): RDD[(Long, Rating)] = {

    /* Read the ratings file */
    sparkSession.sparkContext.textFile(ratingsDataFile).filter(FileUtils.fileHeader).map {

      /* Split the line */
      line =>
        val fields = line.split(",")

        /* Convert each line into <timestamp : Rating<user-id, movie-id, rating>> */
        (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }
  }
}