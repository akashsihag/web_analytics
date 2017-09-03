package com.organization.Analytics

import com.organization.Context.Context
import com.organization.Helpers.CSVHeader

/**
  * @author ${Akash.Sihag}
  */
case class AnalyticsCorrelated(context: Context) extends Analytics {
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
      .map(url => url.replaceFirst("^(http(?>s)://www\\.|http(?>s)://|www\\.)", "").split("/")(0))
      .map(initial_referrer => initial_referrer.replaceFirst("^(http(?>s)://www\\.|http(?>s)://|www\\.)", "").split("/")(0))
    val initial_referring_domains = rows.map(row => header(row, "initial_referring_domain"))

    println("Lead Generator according to events : ")
    println("###########################################################")
    println("\n")

    val sourceRdd = initial_referring_domains.zip(events)
      .map(row => (" Initial Referrer Domain : " + row._1, " Event : " + row._2))

    val eventList = List("Apply Now Click", "Application Step Complete", "Start Application",
      "New Lead Captured", "Submit Application")

    val leadRdd0 = sourceRdd.filter(row => row.toString().contains(eventList(0))).map(_._1)
    val leadRdd1 = sourceRdd.filter(row => row.toString().contains(eventList(1))).map(_._1)
    val leadRdd2 = sourceRdd.filter(row => row.toString().contains(eventList(2))).map(_._1)
    val leadRdd3 = sourceRdd.filter(row => row.toString().contains(eventList(3))).map(_._1)
    val leadRdd4 = sourceRdd.filter(row => row.toString().contains(eventList(4))).map(_._1)

    val leadRddWeight0 = leadRdd0.map(row => (row, 1)).reduceByKey(_ + _).coalesce(1).sortBy(_._2, false)
    val leadRddWeight1 = leadRdd1.map(row => (row, 1)).reduceByKey(_ + _).coalesce(1).sortBy(_._2, false)
    val leadRddWeight2 = leadRdd2.map(row => (row, 1)).reduceByKey(_ + _).coalesce(1).sortBy(_._2, false)
    val leadRddWeight3 = leadRdd3.map(row => (row, 1)).reduceByKey(_ + _).coalesce(1).sortBy(_._2, false)
    val leadRddWeight4 = leadRdd4.map(row => (row, 1)).reduceByKey(_ + _).coalesce(1).sortBy(_._2, false)

    val rdd0 = leadRddWeight0.join(leadRddWeight1).map(row => (row._1, "\n" + eventList(0) + " : " + row._2._1 + "\t" + eventList(1) + " : " + row._2._2))

    val rdd1 = rdd0.join(leadRddWeight2).map(row => (row._1, row._2._1 + "\t" + eventList(2) + " : " + row._2._2))

    val rdd2 = rdd1.join(leadRddWeight3).map(row => (row._1, (row._2._1 + "\t" + eventList(3) + " : " + row._2._2)))

    val rdd3 = rdd2.join(leadRddWeight4).map(row => (row._1, (row._2._1 + "\t" + eventList(4) + " : " + row._2._2)))

    println("###########################################################")
    println("\n")
    rdd3.foreach(row => println(row + "\n"))
    println("###########################################################")
    println("\n\n")
  }
}
