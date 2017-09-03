package com.organization

import java.util.{Calendar, Date}

import com.organization.Context.Context
import com.organization.Helpers.CLIArguments
import com.organization.Analytics._

/**
  * @author ${Akash.Sihag}
  */
object main {
  var endTime: Option[Date] = None

  def main(args: Array[String]) {
    println(" Version 1.0.0 ")
    val arguments: CLIArguments = CLIArguments(args)
    val context = new Context(arguments)

    // trigger run method
    RunAnalytics(context)
  }

  def RunAnalytics(context: Context): Unit = {
    try {
      val analyticsClass = context.argument.SPARK_JOB_NAME match {
        case "IndependentAnalytics" => AnalyticsIndependent(context)
        case "CityWise" => AnalyticsCity(context)
        case "OperatingSystemWise" => AnalyticsOS(context)
        case "RegionWise" => AnalyticsRegion(context)
        case "CourseWise" => AnalyticsCourse(context)
        case "AllParameters" => AnalyticsAllPar(context)
        case "AnalyticsCorrelated" => AnalyticsCorrelated(context)
        case _ => throw new Exception("Job type %s not found".format(context.argument.SPARK_JOB_NAME))
      }
      val startTime = Calendar.getInstance.getTime
      println("Start Time : " + startTime)

      analyticsClass.analyse()

      endTime = Some(Calendar.getInstance.getTime)
      println("End Time : " + endTime)
    }

    catch {
      case exception: Exception => {
        val exceptionMessage = exception.getMessage + "\n" + exception.getStackTrace
        println(exceptionMessage)
        endTime = Some(Calendar.getInstance.getTime)
        println("End Time : " + endTime)
      }
    }
  }

}
