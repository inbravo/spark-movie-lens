package com.inbravo.mr.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

/**
 * amit.dixit
 */
object MoviesUtils {

  val movies = "movies"
  val genres = "genres"
  val genereFantasy = "Fantasy"
  val genereAnimation = "Animation"
  val genereDrama = "Drama"
  val genereWar = "War"
  val genereHorror = "Horror"
  val genereCrime = "Crime"
  val genereMystery = "Mystery"
  val genereAdventure = "Adventure"

  /* Count the number of movies*/
  def movies(sparkSession: SparkSession): DataFrame = {

    /* Select movies, group by generes, count number of records,  */
    sparkSession.sql("select * from " + movies)
  }

  /* Count the number of movies */
  def moviesCount(sparkSession: SparkSession): Long = {

    /* Select movies, group by generes, count number of records  */
    sparkSession.sql("select * from " + movies).count
  }

  /* Count the number of movies */
  def moviesCountByGenre(sparkSession: SparkSession, genre: String): Long = {

    /* Select movies, group by generes, count number of records, filter out unknown genres  */
    sparkSession.sql("select * from " + movies).groupBy(genres).count.filter(row => row.apply(0).toString.contains(genre)).toDF.count
  }

  def moviesByGenre(sparkSession: SparkSession, genre: String): DataFrame = {

    /* Select movies, group by generes, count number of records, filter out unknown genres */
    sparkSession.sql("select * from " + movies).filter(column => column.apply(2).toString.contains(genre)).toDF
  }
}