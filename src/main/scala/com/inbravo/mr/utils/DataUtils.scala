package com.inbravo.mr.utils

import java.util.Date
import org.apache.spark.sql.functions._

/**
 * amit.dixit
 */
object DataUtils {

  /* Create user information from ratings */
  def prepareUsers(sparkSession: org.apache.spark.sql.SparkSession) = {

    val userIds = sparkSession.sql("select distinct userId from ratings").distinct().orderBy(asc("userId"))
    FileUtils.writeAsText(userIds, "users" + new Date().getTime, true)
  }
}