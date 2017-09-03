package com.organization.Analytics

import com.organization.Context.Context
import com.organization.Helpers.CSVHeader

/**
  * @author ${Akash.Sihag}
  */
case class AnalyticsRegion(context: Context) extends Analytics {
  final override def analyse(): Unit = {
    // load the csv file as rdd
    val eventRdd = context.sc.textFile("/home/akash/up/Upgrade/assignment/coding_assignment/input/ugmixp_data.csv", 4)

    // split the csv file
    val splittedRdd = eventRdd.map(line => line.split("\t").map(ele => ele.trim))

    //set header
    val header = new CSVHeader(splittedRdd.take(1)(0))

    //filter header from data
    val rows = splittedRdd.filter(line => header(line, "event") != "event")
    val regions = rows.map(row => header(row, "region"))
    regions.cache()

    //Extract unique counts of each attribute
    println("Region wise analysis : ")
    println("###########################################################")
    println("\n")

    // take top 40 initial_referrer region wise
    val initial_referrers = rows.map(row => header(row, "initial_referrer"))
      .map(initial_referrer => initial_referrer.replaceFirst("^(http(?>s)://www\\.|http(?>s)://|www\\.)", "").split("/")(0))
    val regionWiseInitRef = regions.zip(initial_referrers)
    val regionWiseInitRefWeight = regionWiseInitRef.map(initial_referring_domain => (initial_referring_domain, 1))
    val regionWiseInitRefWeightReduce = regionWiseInitRefWeight.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("Region - Initial referrer relationship : \n")
    println("Total Count : " + regionWiseInitRef.count())
    println("\nShowing top 40\n")
    regionWiseInitRefWeightReduce.foreach(row => println("( Combination : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    // take top 40 referrer region wise
    val referrers = rows.map(row => header(row, "referrer"))
      .map(referrer => referrer.replaceFirst("^(http(?>s)://www\\.|http(?>s)://|www\\.)", "").split("/")(0))
    val regionWiseRef = regions.zip(referrers)
    val regionWiseRefWeight = regionWiseRef.map(initial_referring_domain => (initial_referring_domain, 1))
    val regionWiseRefWeightReduce = regionWiseRefWeight.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("Region - referrer relationship : \n")
    println("Total Count : " + regionWiseRef.count())
    println("\nShowing top 40\n")
    regionWiseRefWeightReduce.foreach(row => println("( Combination : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    // take top 40 course region wise
    val course = rows.map(row => header(row, "Course"))
      .map(referrer => referrer.replaceFirst("^(http(?>s)://www\\.|http(?>s)://|www\\.)", "").split("/")(0))
    val regionWiseCourse = regions.zip(course)
    val regionWiseCourseWeight = regionWiseCourse.map(initial_referring_domain => (initial_referring_domain, 1))
    val regionWiseCourseWeightReduce = regionWiseCourseWeight.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("Region - Course opted relationship : \n")
    println("Total Count : " + regionWiseCourse.count())
    println("\nShowing top 40\n")
    regionWiseCourseWeightReduce.foreach(row => println("( Combination : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    // region wise latest UTM campaign
    val latestUtmCampaign = rows.map(row => header(row, "LatestUTMCampaign"))
      .map(referrer => referrer.replaceFirst("^(http(?>s)://www\\.|http(?>s)://|www\\.)", "").split("/")(0))
    val regionWiseUtmCampaign = regions.zip(latestUtmCampaign)
    val regionWiseUtmCampaignWeight = regionWiseUtmCampaign.map(initial_referring_domain => (initial_referring_domain, 1))
    val regionWiseUtmCampaignWeightReduce = regionWiseUtmCampaignWeight.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("Region - LatestUTMCampaign relationship : \n")
    println("Total Count : " + regionWiseUtmCampaign.count())
    println("\nShowing top 40\n")
    regionWiseUtmCampaignWeightReduce.foreach(row => println("( Combination : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    //     take top 40 UTM source region wise
    val utmSources = rows.map(row => header(row, "utm_source"))
      .map(referrer => referrer.replaceFirst("^(http(?>s)://www\\.|http(?>s)://|www\\.)", "").split("/")(0))
    val regionWiseUtmSources = regions.zip(utmSources)
    val regionWiseUtmSourcesWeight = regionWiseUtmSources.map(initial_referring_domain => (initial_referring_domain, 1))
    val regionWiseUtmSourcesWeightReduce = regionWiseUtmSourcesWeight.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("Region - UtmSources relationship : \n")
    println("Total Count : " + regionWiseUtmSources.count())
    println("\nShowing top 40\n")
    regionWiseUtmSourcesWeightReduce.foreach(row => println("( Combination : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

  }

}
