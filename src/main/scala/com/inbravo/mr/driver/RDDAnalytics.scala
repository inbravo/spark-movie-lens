package com.inbravo.mr.driver

import com.inbravo.mr.config.ProjectConfig
import com.inbravo.mr.utils.FileUtils
import com.inbravo.mr.utils.MovieUtils
import com.inbravo.mr.spark.SparkContextFactory

/**
 * amit.dixit
 */
object RDDAnalytics extends ProjectConfig {

  def main(args: Array[String]): Unit = {

    /* Get movie recommendation */
    MovieUtils.recommendMovies(SparkContextFactory.getSparkSession("local"))
  }
}