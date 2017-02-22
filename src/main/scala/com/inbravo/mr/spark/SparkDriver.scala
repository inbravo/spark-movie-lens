package com.inbravo.mr.spark

import java.util.Date
import com.inbravo.mr.config.ProjectConfig
import com.inbravo.mr.utils.FileUtils
import com.inbravo.mr.utils.DataUtils
import com.inbravo.mr.utils.MoviesUtils

/**
 * amit.dixit
 * Main executor class
 */
object SparkDriver extends ProjectConfig {

  def main(args: Array[String]): Unit = {

    /* To avoid 'winutils.exe' error */
    System.setProperty("hadoop.home.dir", winUtils);

    /* Create local spark session */
    val sparkSession = SparkContextFactory.getSparkSession("local")

    /* Read all GroupLens Research data; downloaded in zip format */
    FileUtils.readAsCSV(moviesData, sparkSession).createOrReplaceTempView(MoviesUtils.movies)

    /* Count all Mystery movies */
    println("----------------------------------------------------")
    println("Count of all movies: " + MoviesUtils.moviesCount(sparkSession))

    /* Count all Mystery movies */
    println("----------------------------------------------------")
    println("All movies: ")
    MoviesUtils.movies(sparkSession)

    /* Count all Mystery movies */
    println("----------------------------------------------------")
    println("Count of Mystery movies: " + MoviesUtils.moviesCountByGenre(sparkSession, genre = "Mystery"))

    /* Get all Fantasy movies */
    println("----------------------------------------------------")
    println("Fantasy movies: ")
    MoviesUtils.moviesByGenre(sparkSession, genre = "Fantasy").show

    /* Get all Animation movies */
    println("----------------------------------------------------")
    println("Animation movies: ")
    MoviesUtils.moviesByGenre(sparkSession, genre = "Animation").show

    /* Get all Animation movies */
    println("----------------------------------------------------")
    println("Drama movies: ")
    MoviesUtils.moviesByGenre(sparkSession, genre = "Drama").show

    /* Get all Animation movies */
    println("----------------------------------------------------")
    println("War movies: ")
    MoviesUtils.moviesByGenre(sparkSession, genre = "War").show
  }
}