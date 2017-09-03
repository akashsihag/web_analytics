package com.organization.Context

import com.organization.Helpers.CLIArguments
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author ${Akash.Sihag}
  */
case class Context(argument: CLIArguments) {
  val conf = if (argument.SPARK_CONTEXT_MODE == "LOCAL") {
    new SparkConf().setAppName(argument.SPARK_JOB_NAME).setMaster("local[*]")
  }
  else {
    new SparkConf().setAppName(argument.SPARK_JOB_NAME).setMaster(argument.MASTER_URL)
  }
  val sc = new SparkContext(conf)
}
