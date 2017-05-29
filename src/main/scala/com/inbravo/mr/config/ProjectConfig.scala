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
  val moviesDataFile = data.getString("movies.file")
  val ratingsDataFile = data.getString("ratings.file")
  val tagsDataFile = data.getString("tags.file")
  val usersDataFile = data.getString("users.file")
}