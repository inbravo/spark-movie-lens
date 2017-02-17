package com.inbravo.mr.spark

import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerTaskGettingResult
import org.apache.spark.scheduler.SparkListenerBlockManagerRemoved
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.scheduler.SparkListenerUnpersistRDD
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.scheduler.SparkListenerApplicationEnd
import org.apache.spark.scheduler.SparkListenerApplicationStart
import org.apache.spark.scheduler.SparkListenerExecutorMetricsUpdate
import org.apache.spark.scheduler.SparkListenerTaskStart
import org.apache.spark.scheduler.SparkListenerExecutorRemoved
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListenerBlockUpdated
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.SparkListenerExecutorAdded
import org.apache.spark.scheduler.SparkListenerBlockManagerAdded

class MySparkListener extends SparkListener {
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    println("On Stage Completed " + stageCompleted.stageInfo.name + " Details" + stageCompleted.stageInfo.details)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    println("On Stage Submitted " + stageSubmitted.stageInfo.name + " Details" + stageSubmitted.stageInfo.details)
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {}

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {}

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {}

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    println("On Job Start " + jobStart.jobId + " " + jobStart.stageInfos)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    println("On Job end " + jobEnd.jobId + " " + jobEnd.jobResult)
  }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {}

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {}

  override def onBlockManagerRemoved(
    blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {}

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {}

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {}

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {}

  override def onExecutorMetricsUpdate(
    executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {}

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {}

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {}

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {}

  override def onOtherEvent(event: SparkListenerEvent): Unit = {}
}