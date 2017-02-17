package com.inbravo.mr.utils

import org.apache.spark.sql.SparkSession

/**
 * amit.dixit
 */
object MoviesUtils {

  def moviesCountByGenre(movies: String, sparkSession: SparkSession): Unit = {
    sparkSession.sql("select * from " + movies).groupBy("genres").count.filter(row => row.apply(0).toString.contains("Mystery|IMAX")).toDF.show
  }

  def moviesByGenre(movies: String, sparkSession: SparkSession, genre: String = "Drama|Romance"): Unit = {
    sparkSession.sql("select genres, title from " + movies).filter(column => column.apply(0).toString.contains(genre)).toDF.show
  }
}