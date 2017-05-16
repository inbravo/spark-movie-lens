package com.inbravo.mr.utils

import org.apache.spark.sql.{ DataFrame, SparkSession }
import com.inbravo.mr.config.ProjectConfig
/**
 * 	amit.dixit
 *  File handling traits
 */
trait CustomFileReader { def readAsCSV(fileName: String, sparkSession: SparkSession): DataFrame }
trait CustomFileWriter {
  def writeAsText(df: DataFrame, location: String, writeSingle: Boolean)
  def writeAsCSV(df: DataFrame, location: String, writeSingle: Boolean)
}

/**
 * amit.dixit
 *
 * Final objects
 */
object FileUtils extends CustomFileReader with CustomFileWriter with ProjectConfig {

  /* Read operation(s) */
  override def readAsCSV(filename: String, sparkSession: SparkSession): DataFrame = sparkSession.read.option("header", "true").option("mode", "DROPMALFORMED").format("com.databricks.spark.csv").csv(filename)

  /* Write operation(s) */
  override def writeAsText(df: DataFrame, location: String, writeSingle: Boolean) = { if (writeSingle) { df.coalesce(1).rdd.saveAsTextFile("users") } }
  override def writeAsCSV(df: DataFrame, location: String, writeSingle: Boolean) = { if (writeSingle) { df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(location) } }

  /* Discard file headers */
  def discardFileHeader(line: String): Boolean = {

    if (line.contains("movieId") | line.contains("userId") | line.contains("rating") | line.contains("timestamp")) false else true
  }
}