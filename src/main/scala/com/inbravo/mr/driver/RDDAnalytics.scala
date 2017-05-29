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

    /* Count first ten movies */
    println("----------------------------------------------------")
    println("First Ten Movies: " + MovieUtils.firstTenMoviesRDD(SparkContextFactory.getSparkSession("local")).foreach { x => println(x) })

    /* All 'action' movies */
    println("----------------------------------------------------")
    println("All Action Movies: " + MovieUtils.moviesRDDByGenre(SparkContextFactory.getSparkSession("local"), genre = MovieUtils.genereAction).foreach { x => println(x) })
  }
}