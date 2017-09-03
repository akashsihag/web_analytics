package com.organization.Analytics

import com.organization.Context.Context
import com.organization.Helpers.CSVHeader

/**
  * @author ${Akash.Sihag}
  */
case class AnalyticsOS(context: Context) extends Analytics {
  final override def analyse(): Unit = {
    // load the csv file as rdd
    val eventRdd = context.sc.textFile("/home/akash/up/Upgrade/assignment/coding_assignment/input/ugmixp_data.csv", 4)

    // split the csv file
    val splittedRdd = eventRdd.map(line => line.split("\t").map(ele => ele.trim))

    //set header
    val header = new CSVHeader(splittedRdd.take(1)(0))

    //filter header from data
    val rows = splittedRdd.filter(line => header(line, "event") != "event")
    val operatingSystems = rows.map(row => header(row, "os"))
    operatingSystems.cache()

    //Extract unique counts of each attribute
    println("Analysis on the basis of Operating system used by user : ")
    println("###########################################################")

    // take top 40 initial_referrer os wise
    val initial_referrers = rows.map(row => header(row, "initial_referrer"))
      .map(initial_referrer => initial_referrer.replaceFirst("^(http(?>s)://www\\.|http(?>s)://|www\\.)", "").split("/")(0))
    val osWiseInitRef = operatingSystems.zip(initial_referrers)
    val osWiseInitRefWeight = osWiseInitRef.map(initial_referring_domain => (initial_referring_domain, 1))
    val osWiseInitRefWeightReduce = osWiseInitRefWeight.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("Operating System - Initial referrer relationship : \n")
    println("Total Count : " + osWiseInitRef.count())
    println("\nShowing top 40\n")
    osWiseInitRefWeightReduce.foreach(row => println("( Combination : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    // take top 40 referrer os wise
    val referrers = rows.map(row => header(row, "referrer"))
      .map(referrer => referrer.replaceFirst("^(http(?>s)://www\\.|http(?>s)://|www\\.)", "").split("/")(0))
    val osWiseRef = operatingSystems.zip(referrers)
    val osWiseRefWeight = osWiseRef.map(initial_referring_domain => (initial_referring_domain, 1))
    val osWiseRefWeightReduce = osWiseRefWeight.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("Operating System - referrer relationship : \n")
    println("Total Count : " + osWiseRef.count())
    println("\nShowing top 40\n")
    osWiseRefWeightReduce.foreach(row => println("( Combination : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    // take top 40 course os wise
    val course = rows.map(row => header(row, "Course"))
      .map(referrer => referrer.replaceFirst("^(http(?>s)://www\\.|http(?>s)://|www\\.)", "").split("/")(0))
    val osWiseCourse = operatingSystems.zip(course)
    val osWiseCourseWeight = osWiseCourse.map(initial_referring_domain => (initial_referring_domain, 1))
    val osWiseCourseWeightReduce = osWiseCourseWeight.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("Operating System - Course relationship : \n")
    println("Total Count : " + osWiseCourse.count())
    println("\nShowing top 40\n")
    osWiseCourseWeightReduce.foreach(row => println("( Combination : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    // os wise latest UTM campaign
    val latestUtmCampaign = rows.map(row => header(row, "LatestUTMCampaign"))
      .map(referrer => referrer.replaceFirst("^(http(?>s)://www\\.|http(?>s)://|www\\.)", "").split("/")(0))
    val osWiseUtmCampaign = operatingSystems.zip(latestUtmCampaign)
    val osWiseUtmCampaignWeight = osWiseUtmCampaign.map(initial_referring_domain => (initial_referring_domain, 1))
    val osWiseUtmCampaignWeightReduce = osWiseUtmCampaignWeight.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("os - LatestUTMCampaign relationship : \n")
    println("Total Count : " + osWiseUtmCampaign.count())
    println("\nShowing top 40\n")
    osWiseUtmCampaignWeightReduce.foreach(row => println("( Combination : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    //     take top 40 UTM source os wise
    val utmSources = rows.map(row => header(row, "utm_source"))
      .map(referrer => referrer.replaceFirst("^(http(?>s)://www\\.|http(?>s)://|www\\.)", "").split("/")(0))
    val osWiseUtmSources = operatingSystems.zip(utmSources)
    val osWiseUtmSourcesWeight = osWiseUtmSources.map(initial_referring_domain => (initial_referring_domain, 1))
    val osWiseUtmSourcesWeightReduce = osWiseUtmSourcesWeight.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("Operating System - UtmSources relationship : \n")
    println("Total Count : " + osWiseUtmSources.count())
    println("\nShowing top 40\n")
    osWiseUtmSourcesWeightReduce.foreach(row => println("( Combination : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")
  }

}
