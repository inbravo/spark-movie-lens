package com.inbravo.mr.config

import com.typesafe.config.ConfigFactory
/**
 * 	amit.dixit
 */
trait ProjectConfig {

  private val config = ConfigFactory.load()

  private val hadoop = config.getConfig("hadoop")
  private val data = config.getConfig("data")

  val winUtils = hadoop.getString("winutils")
  val moviesData = data.getString("movies.file")
  val ratingsData = data.getString("ratings.file")
  val tagsData = data.getString("tags.file")
  val usersData = data.getString("users.file")
}