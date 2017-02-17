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
  val movies = data.getString("movies.file")
  val ratings = data.getString("ratings.file")
  val tags = data.getString("tags.file")
  val users = data.getString("users.file")
}