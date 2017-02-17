package com.inbravo.mr.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
/**
 * 	amit.dixit
 */
class SparkContextFactory {

  def getSparkContext(environment: String): SparkContext = {

    val sparkConfig = prepareSparkConfig(environment)
    var sparkContext: SparkContext = null
    if (sparkConfig != null) {
      sparkContext = new SparkContext(sparkConfig)
    }
    sparkContext
  }

  def prepareSparkConfig(environment: String): SparkConf = {

    var config: SparkConf = null
    if (environment != null && (environment.equalsIgnoreCase("local")
      || environment.equalsIgnoreCase("standalone"))) {

      config = new SparkConf().setMaster("local[*]").setAppName("Movies Recommendation")

    } else if (environment != null && (environment.equalsIgnoreCase("yarn")
      || environment.equalsIgnoreCase("cluster"))) {
      /* TODO */
    }
    return config
  }

  def getSparkSession(environment: String): SparkSession = {
    val sparkConf = prepareSparkConfig(environment)
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    setProperties(sparkSession)
    // For implicit conversions like converting RDDs to DataFrames
    import sparkSession.implicits._
    sparkSession
  }

  //Set all Application properties for spark session
  def setProperties(sparkSession: SparkSession) = {

    /* Print the spark version */
    println("=**=Spark version: " + sparkSession.sparkContext.version + "=**=")

    /* Change log level to avoid lots of log */
    sparkSession.sparkContext.setLogLevel("ERROR")

    sparkSession.conf.set("spark.app.name", "Movies Recommendation") //Default None
    sparkSession.conf.set("spark.master", "local") //Default None
    sparkSession.conf.set("spark.driver.cores", "1") //Default 1, used in cluster
    sparkSession.conf.set("spark.driver.maxResultSize", "1g") //default 1g
    sparkSession.conf.set("spark.driver.memory", "1g") //Default 1g
    sparkSession.conf.set("spark.executor.memory", "1g") //Default 1g
    sparkSession.conf.set("spark.extraListeners", "com.inbravo.mr.spark.MySparkListener") //Default None
    sparkSession.conf.set("spark.local.dir", "staging") //Default /temp
    sparkSession.conf.set("spark.logConf", "true") // false
    sparkSession.conf.set("spark.submit.deployMode", "client") //Default None, client for local, cluster for cluster environment

    //Port on which the external shuffle service will run. 
    sparkSession.conf.set("spark.shuffle.service.port", "7337") //Default 7337

    //Base directory in which Spark events are logged, if spark.eventLog.enabled is true. 
    //Within this base directory, Spark creates a sub-directory for each application, and logs the events specific to the application in this directory. 
    //Users may want to set this to a unified location like an HDFS directory so history files can be read by the history server.
    sparkSession.conf.set("spark.eventLog.dir", "event-log-dir") //Default
    //Whether to log Spark events, useful for reconstructing the Web UI after the application has finished. 
    sparkSession.conf.set("spark.eventLog.enabled", "true") //Default false
    //Spark UI port
    sparkSession.conf.set("spark.ui.port", "4040") //Default 4040
  }
}