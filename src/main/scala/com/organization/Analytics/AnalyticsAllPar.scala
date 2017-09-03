package com.organization.Analytics

import com.organization.Context.Context
import com.organization.Helpers.CSVHeader

/**
  * @author ${Akash.Sihag}
  */
final case class AnalyticsAllPar(context: Context) extends Analytics {
  override def analyse(): Unit = {

    // load the csv file as rdd
    val eventRdd = context.sc.textFile("/home/akash/up/Upgrade/assignment/coding_assignment/input/ugmixp_data.csv", 4)

    // split the csv file
    val splittedRdd = eventRdd.map(line => line.split("\t").map(ele => ele.trim))

    //set header
    val header = new CSVHeader(splittedRdd.take(1)(0))

    //filter header from data
    val rows = splittedRdd.filter(line => header(line, "event") != "event")
    rows.cache()

    val events = rows.map(row => header(row, "event"))
    val current_urls = rows.map(row => header(row, "current_url"))
      .map(url => url.replaceFirst("^(http(?>s)://www\\.|http(?>s)://|www\\.)", "").split("/")(0))
    val cities = rows.map(row => header(row, "city"))
    val initial_referrers = rows.map(row => header(row, "initial_referrer"))
      .map(initial_referrer => initial_referrer.replaceFirst("^(http(?>s)://www\\.|http(?>s)://|www\\.)", "").split("/")(0))
    val initial_referring_domains = rows.map(row => header(row, "initial_referring_domain"))
    val oss = rows.map(row => header(row, "os"))
    val referrers = rows.map(row => header(row, "referrer"))
    val referring_domains = rows.map(row => header(row, "referring_domain"))
    val regions = rows.map(row => header(row, "region"))
    val Courses = rows.map(row => header(row, "Course"))
    val latestUTMCampaigns = rows.map(row => header(row, "LatestUTMCampaign"))
    val utm_sources = rows.map(row => header(row, "utm_source"))

    println("Considering various attributes: (Course, Current_urls, City, Region, Oerating System, Initial Referrer, Referrer, Tag used for the campaign, Source of the campaign)")
    println("###########################################################")
    println("\n")

    // zip attributes together
    val correlatedRdd = referrers.zip(initial_referrers).map(row => (" Referrer : " + row._1, " Initial Referrer : " + row._2))
      .zip(Courses).map(row => (row._1._1, row._1._2, " Course : " + row._2))
      .zip(oss).map(row => (row._1._1, row._1._2, row._1._3, " Operatinig System: " + row._2))
      .zip(latestUTMCampaigns).map(row => (row._1._1, row._1._2, row._1._3, row._1._4, " Tag used for the campaign : " + row._2))
      .zip(utm_sources).map(row => (row._1._1, row._1._2, row._1._3, row._1._4, row._1._5, " Source of the campaign : " + row._2))
      .zip(cities).map(row => (row._1._1, row._1._2, row._1._3, row._1._4, row._1._5, row._1._6, " City : " + row._2))
      .zip(regions).map(row => (row._1._1, row._1._2, row._1._3, row._1._4, row._1._5, row._1._6, row._1._7, " Region : " + row._2))
      .zip(current_urls).map(row => (row._1._1, row._1._2, row._1._3, row._1._4, row._1._5, row._1._6, row._1._7, row._1._8, " Current URL : " + row._2))

    val correlatedRddWeight = correlatedRdd.map(row => (row, 1))
    val correlatedRddWeightReduce = correlatedRddWeight.reduceByKey(_ + _)
      .coalesce(1).sortBy(_._2, false)
    println("Total Count : " + correlatedRdd.count())
    println("###########################################################")
    println("\n")
    correlatedRddWeightReduce.foreach(row => println("( Combination ==> " + row._1 + ",\nCount ==> " + row._2 + " )\n\n"))
    println("###########################################################")
    println("\n\n")
  }
}
