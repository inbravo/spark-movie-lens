package com.inbravo.mr.driver

import com.inbravo.mr.config.ProjectConfig
import com.inbravo.mr.utils.FileUtils
import com.inbravo.mr.utils.MovieUtils
import com.inbravo.mr.spark.SparkContextFactory

/**
 * amit.dixit
 *
 * Main executor class
 */
object DataFrameAnalytics extends ProjectConfig {

  def main(args: Array[String]): Unit = {

    /* Create local spark session */
    val sparkSession = SparkContextFactory.getSparkSession("local")

    /* Read all GroupLens Research data; downloaded in zip format */
    FileUtils.readAsCSV(moviesDataFile, sparkSession).createOrReplaceTempView(MovieUtils.moviesDF)

    /* Count all movies */
    println("----------------------------------------------------")
    println("Count of all movies: " + MovieUtils.moviesCount(sparkSession))

    /* Count all Mystery movies */
    println("----------------------------------------------------")
    println("Count of Mystery movies: " + MovieUtils.moviesCountByGenre(sparkSession, genre = MovieUtils.genereMystery))

    /* Get all Fantasy movies */
    println("----------------------------------------------------")
    println("Fantasy movies: ")
    MovieUtils.moviesDFByGenre(sparkSession, genre = MovieUtils.genereFantasy).show

    /* Get all Animation movies */
    println("----------------------------------------------------")
    println("Animation movies: ")
    MovieUtils.moviesDFByGenre(sparkSession, genre = MovieUtils.genereAnimation).show

    /* Get all Drama movies */
    println("----------------------------------------------------")
    println("Drama movies: ")
    MovieUtils.moviesDFByGenre(sparkSession, genre = MovieUtils.genereDrama).show

    /* Count all War movies */
    println("----------------------------------------------------")
    val warMovies = MovieUtils.moviesDFByGenre(sparkSession, genre = MovieUtils.genereWar)
    println("Count of War movies: " + warMovies.count)

    /* Get all War movies */
    println("----------------------------------------------------")
    println("War movies: ")
    warMovies.foreach(movie => println(movie))

    /* Show all movies */
    println("----------------------------------------------------")
    println("All movies: ")
    MovieUtils.moviesDF(sparkSession).foreach(movie => println(movie))
    
    MovieUtils.recommendMovies(sparkSession)
  }
}