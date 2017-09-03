package com.organization.Analytics

import com.organization.Context.Context
import com.organization.Helpers.CSVHeader

/**
  * @author ${Akash.Sihag}
  */
case class AnalyticsCourse(context: Context) extends Analytics {
  final override def analyse(): Unit = {
    // load the csv file as rdd
    val eventRdd = context.sc.textFile("/home/akash/up/Upgrade/assignment/coding_assignment/input/ugmixp_data.csv", 4)

    // split the csv file
    val splittedRdd = eventRdd.map(line => line.split("\t").map(ele => ele.trim))

    //set header
    val header = new CSVHeader(splittedRdd.take(1)(0))

    //filter header from data
    val rows = splittedRdd.filter(line => header(line, "event") != "event")
    val courses = rows.map(row => header(row, "Course"))
    courses.cache()

    //Extract unique counts of each attribute
    println("Analysis on the basis of Courses : ")
    println("###########################################################")


    // take top 40 initial_referrer course wise
    val initial_referrers = rows.map(row => header(row, "initial_referrer"))
      .map(initial_referrer => initial_referrer.replaceFirst("^(http(?>s)://www\\.|http(?>s)://|www\\.)", "").split("/")(0))
    val courseWiseInitRef = courses.zip(initial_referrers)
    val courseWiseInitRefWeight = courseWiseInitRef.map(initial_referring_domain => (initial_referring_domain, 1))
    val courseWiseInitRefWeightReduce = courseWiseInitRefWeight.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("Course - Initial referrer relationship : \n")
    println("Total Count : " + courseWiseInitRef.count())
    println("\nShowing top 40\n")
    courseWiseInitRefWeightReduce.foreach(row => println("( Combination : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    // take top 40 referrer course wise
    val referrers = rows.map(row => header(row, "referrer"))
      .map(referrer => referrer.replaceFirst("^(http(?>s)://www\\.|http(?>s)://|www\\.)", "").split("/")(0))
    val courseWiseRef = courses.zip(referrers)
    val courseWiseRefWeight = courseWiseRef.map(initial_referring_domain => (initial_referring_domain, 1))
    val courseWiseRefWeightReduce = courseWiseRefWeight.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("Course - referrer relationship : \n")
    println("Total Count : " + courseWiseRef.count())
    println("\nShowing top 40\n")
    courseWiseRefWeightReduce.foreach(row => println("( Combination : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    // course wise latest UTM campaign
    val latestUtmCampaign = rows.map(row => header(row, "LatestUTMCampaign"))
      .map(referrer => referrer.replaceFirst("^(http(?>s)://www\\.|http(?>s)://|www\\.)", "").split("/")(0))
    val courseWiseUtmCampaign = courses.zip(latestUtmCampaign)
    val courseWiseUtmCampaignWeight = courseWiseUtmCampaign.map(initial_referring_domain => (initial_referring_domain, 1))
    val courseWiseUtmCampaignWeightReduce = courseWiseUtmCampaignWeight.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("Course - LatestUTMCampaign relationship : \n")
    println("Total Count : " + courseWiseUtmCampaign.count())
    println("\nShowing top 40\n")
    courseWiseUtmCampaignWeightReduce.foreach(row => println("( Combination : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    //     take top 40 UTM source course wise
    val utmSources = rows.map(row => header(row, "utm_source"))
      .map(referrer => referrer.replaceFirst("^(http(?>s)://www\\.|http(?>s)://|www\\.)", "").split("/")(0))
    val courseWiseUtmSources = courses.zip(utmSources)
    val courseWiseUtmSourcesWeight = courseWiseUtmSources.map(initial_referring_domain => (initial_referring_domain, 1))
    val courseWiseUtmSourcesWeightReduce = courseWiseUtmSourcesWeight.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("Course - UtmSources relationship : \n")
    println("Total Count : " + courseWiseUtmSources.count())
    println("\nShowing top 40\n")
    courseWiseUtmSourcesWeightReduce.foreach(row => println("( Combination : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")
  }

}
