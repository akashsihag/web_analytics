package com.organization.Helpers

/**
  * @author ${Akash.Sihag}
  */
case class CLIArguments(SPARK_JOB_NAME: String = "", PARTITIONS: Int = 4,
                        SPARK_CONTEXT_MODE: String = "Local", MASTER_URL: String = "") {
  println((" SPARK_JOB_TYPE : %s\n PARTITION : %s\n SPARK_CONTEXT_TYPE : %s\n")
    .format(SPARK_JOB_NAME, PARTITIONS.toString, SPARK_CONTEXT_MODE, MASTER_URL))
}

object CLIArguments {
  def apply(args: Array[String]) = {
    if (args.length == 3) {
      new CLIArguments(args(0), args(1).toInt, args(2))
    } else if (args.length == 4) {
      new CLIArguments(args(0), args(1).toInt, args(2), args(3))
    }
    else {
      throw new IllegalArgumentException("3 arguments expected, %s received.".format(args.length))
    }
  }
}
