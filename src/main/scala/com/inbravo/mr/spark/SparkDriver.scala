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

    val sparkSession = new SparkContextFactory().getSparkSession("local")

    /* Read all GroupLens Research data; downloaded in zip format */
    FileUtils.readAsCSV(movies, sparkSession).createOrReplaceTempView("movies")

    /* Perform movie operations */
    MoviesUtils.moviesCountByGenre("movies", sparkSession)
    MoviesUtils.moviesByGenre("movies", sparkSession, genre = "Fantasy")
    MoviesUtils.moviesByGenre("movies", sparkSession)
  }
}