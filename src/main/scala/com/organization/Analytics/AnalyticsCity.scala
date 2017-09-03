package com.organization.Analytics

import com.organization.Context.Context
import com.organization.Helpers.CSVHeader

/**
  * @author ${Akash.Sihag}
  */
final case class AnalyticsCity(context: Context) extends Analytics {
  final override def analyse(): Unit = {

    // load the csv file as rdd
    val eventRdd = context.sc.textFile("/home/akash/up/Upgrade/assignment/coding_assignment/input/ugmixp_data.csv", 4)

    // split the csv file
    val splittedRdd = eventRdd.map(line => line.split("\t").map(ele => ele.trim))

    //set header
    val header = new CSVHeader(splittedRdd.take(1)(0))

    //filter header from data
    val rows = splittedRdd.filter(line => header(line, "event") != "event")
    val cities = rows.map(row => header(row, "city"))
    cities.cache()

    println("Analysis on the basis of city: ")
    println("###########################################################")
    println("\n\n")


    // take top 40 initial_referrer city wise
      val initial_referrers = rows.map(row => header(row, "initial_referrer"))
      .map(initial_referrer => initial_referrer.replaceFirst("^(http(?>s)://www\\.|http(?>s)://|www\\.)", "").split("/")(0))
    val cityWiseInitRef = cities.zip(initial_referrers)
    val cityWiseInitRefWeight = cityWiseInitRef.map(initial_referring_domain => (initial_referring_domain, 1))
    val cityWiseInitRefWeightReduce = cityWiseInitRefWeight.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("City - Initial referrer relationship : \n")
    println("Total Count : " + cityWiseInitRef.count())
    println("\nShowing top 40\n")
    cityWiseInitRefWeightReduce.foreach(row => println("( Combination : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    // take top 40 referrer city wise
    val referrers = rows.map(row => header(row, "referrer"))
      .map(referrer => referrer.replaceFirst("^(http(?>s)://www\\.|http(?>s)://|www\\.)", "").split("/")(0))
    val cityWiseRef = cities.zip(referrers)
    val cityWiseRefWeight = cityWiseRef.map(initial_referring_domain => (initial_referring_domain, 1))
    val cityWiseRefWeightReduce = cityWiseRefWeight.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("City - referrer relationship : \n")
    println("Total Count : " + cityWiseRef.count())
    println("\nShowing top 40\n")
    cityWiseRefWeightReduce.foreach(row => println("( Combination : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    // take top 40 course city wise
    val course = rows.map(row => header(row, "Course"))
      .map(referrer => referrer.replaceFirst("^(http(?>s)://www\\.|http(?>s)://|www\\.)", "").split("/")(0))
    val cityWiseCourse = cities.zip(course)
    val cityWiseCourseWeight = cityWiseCourse.map(initial_referring_domain => (initial_referring_domain, 1))
    val cityWiseCourseWeightReduce = cityWiseCourseWeight.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("City - Course relationship : \n")
    println("Total Count : " + cityWiseCourse.count())
    println("\nShowing top 40\n")
    cityWiseCourseWeightReduce.foreach(row => println("( Combination : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    // city wise latest UTM campaign
    val latestUtmCampaign = rows.map(row => header(row, "LatestUTMCampaign"))
      .map(referrer => referrer.replaceFirst("^(http(?>s)://www\\.|http(?>s)://|www\\.)", "").split("/")(0))
    val cityWiseUtmCampaign = cities.zip(latestUtmCampaign)
    val cityWiseUtmCampaignWeight = cityWiseUtmCampaign.map(initial_referring_domain => (initial_referring_domain, 1))
    val cityWiseUtmCampaignWeightReduce = cityWiseUtmCampaignWeight.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("City - LatestUTMCampaign relationship : \n")
    println("Total Count : " + cityWiseUtmCampaign.count())
    println("\nShowing top 40\n")
    cityWiseUtmCampaignWeightReduce.foreach(row => println("( Combination : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    //     take top 40 UTM source city wise
    val utmSources = rows.map(row => header(row, "utm_source"))
      .map(referrer => referrer.replaceFirst("^(http(?>s)://www\\.|http(?>s)://|www\\.)", "").split("/")(0))
    val cityWiseUtmSources = cities.zip(utmSources)
    val cityWiseUtmSourcesWeight = cityWiseUtmSources.map(initial_referring_domain => (initial_referring_domain, 1))
    val cityWiseUtmSourcesWeightReduce = cityWiseUtmSourcesWeight.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("City - UtmSources relationship : \n")
    println("Total Count : " + cityWiseUtmSources.count())
    println("\nShowing top 40\n")
    cityWiseUtmSourcesWeightReduce.foreach(row => println("( Combination : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

  }

}
