package com.inbravo.mr.config

import com.typesafe.config.ConfigFactory
/**
 * 	amit.dixit
 */
trait ProjectConfig {

  private val config = ConfigFactory.load()

  private val hadoop = config.getConfig("hadoop")
  private val data = config.getConfig("data")
  
  /* Required to run on windows machine */
  val winUtils = hadoop.getString("winutils")
  
  /* Dataset from 'http://files.grouplens.org/datasets/movielens/ml-20m.zip' */
  val moviesData_ML_20M = data.getString("movies.file")
  val ratingsData_ML_20M = data.getString("ratings.file")
  val tagsData_ML_20M = data.getString("tags.file")
  val usersData_ML_20M = data.getString("users.file")
}