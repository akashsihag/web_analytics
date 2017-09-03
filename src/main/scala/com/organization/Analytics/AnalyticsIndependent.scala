package com.organization.Analytics

import com.organization.Context.Context
import com.organization.Helpers.CSVHeader

/**
  * @author ${Akash.Sihag}
  */
case class AnalyticsIndependent(context: Context) extends Analytics {
  final override def analyse(): Unit = {
    // load the csv file as rdd
    val eventRdd = context.sc.textFile("/home/akash/up/Upgrade/assignment/coding_assignment/input/ugmixp_data.csv", 4)

    // split the csv file
    val splittedRdd = eventRdd.map(line => line.split("\t").map(ele => ele.trim))

    //set header
    val header = new CSVHeader(splittedRdd.take(1)(0))

    //filter header from data
    val rows = splittedRdd.filter(line => header(line, "event") != "event")
    rows.cache()

    //Extract unique counts of each attribute
    println("Independent Analysis of attributes: ")
    println("###########################################################")

    val events = rows.map(row => header(row, "event")).map(event => (event, 1))
    val eventresult = events.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("Events : \n")
    eventresult.foreach(row => println("( Event : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    //    val timestamps = rows.map(row => header(row, "timestamp")).map(timestamp => (timestamp, 1))
    //    val timestampresult = timestamps.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false)
    //    println("timestamp : \n")
    //    timestampresult.foreach(row => println("( Event : "+ row._1 + ",\tCount : " + row._2 + " )"))
    //    println("###########################################################")
    //    println("\n\n")

    val distinct_ids = rows.map(row => header(row, "distinct_id")).map(distinct_id => (distinct_id, 1))
    val distinct_idresult = distinct_ids.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("distinct_ids : \n")
    distinct_idresult.foreach(row => println("( distinct_id : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    val cities = rows.map(row => header(row, "city")).map(citycity => (citycity, 1))
    val citiesresult = cities.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("cities : \n")
    citiesresult.foreach(row => println("( city : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    val current_urls = rows.map(row => header(row, "current_url")).map(current_url => (current_url, 1))
    val current_urlresult = current_urls.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("current_urls : \n")
    current_urlresult.foreach(row => println("( current_url : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    val initial_referrers = rows.map(row => header(row, "initial_referrer"))
      .map(initial_referrer => initial_referrer.replaceFirst("^(http(?>s)://www\\.|http(?>s)://|www\\.)", "").split("/")(0))
      .map(initial_referrer => (initial_referrer, 1))
    val initial_referrerresult = initial_referrers.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("initial_referrers : \n")
    initial_referrerresult.foreach(row => println("( initial_referrer : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    val initial_referring_domains = rows.map(row => header(row, "initial_referring_domain"))
      .map(initial_referring_domain => initial_referring_domain.replaceFirst("^(http(?>s)://www\\.|http(?>s)://|www\\.)", "").split("/")(0))
      .map(initial_referring_domain => (initial_referring_domain, 1))
    val initial_referring_domainresult = initial_referring_domains.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("initial_referring_domains : \n")
    initial_referring_domainresult.foreach(row => println("( initial_referring_domain : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    val oss = rows.map(row => header(row, "os")).map(os => (os, 1))
    val osresult = oss.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("operating systems : \n")
    osresult.foreach(row => println("( operating system : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    val referrers = rows.map(row => header(row, "referrer"))
      .map(referrer => (referrer, 1))
    val referrerresult = referrers.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("referrers : \n")
    referrerresult.foreach(row => println("( referrer : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    val referring_domains = rows.map(row => header(row, "referring_domain")).map(referring_domain => (referring_domain, 1))
    val referring_domainresult = referring_domains.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("referring_domains : \n")
    referring_domainresult.foreach(row => println("( referring_domain : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    val regions = rows.map(row => header(row, "region")).map(region => (region, 1))
    val regionresult = regions.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("regions : \n")
    regionresult.foreach(row => println("( region : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    val Courses = rows.map(row => header(row, "Course")).map(Course => (Course, 1))
    val Courseresult = Courses.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("Courses : \n")
    Courseresult.foreach(row => println("( Course : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    val LatestUTMCampaigns = rows.map(row => header(row, "LatestUTMCampaign")).map(LatestUTMCampaign => (LatestUTMCampaign, 1))
    val LatestUTMCampaignresult = LatestUTMCampaigns.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("LatestUTMCampaigns : \n")
    LatestUTMCampaignresult.foreach(row => println("( LatestUTMCampaign : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    val LatestUTMContents = rows.map(row => header(row, "LatestUTMContent")).map(event => (event, 1))
    val LatestUTMContentresult = LatestUTMContents.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("LatestUTMContents : \n")
    LatestUTMContentresult.foreach(row => println("( LatestUTMContent : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    val LatestUTMMediums = rows.map(row => header(row, "LatestUTMMedium")).map(LatestUTMMedium => (LatestUTMMedium, 1))
    val LatestUTMMediumresult = LatestUTMMediums.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("LatestUTMMediums : \n")
    LatestUTMMediumresult.foreach(row => println("( LatestUTMMedium : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    val LatestUTMSources = rows.map(row => header(row, "LatestUTMSource")).map(LatestUTMSource => (LatestUTMSource, 1))
    val LatestUTMSourceresult = LatestUTMSources.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("LatestUTMSources : \n")
    LatestUTMSourceresult.foreach(row => println("( LatestUTMSource : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    val PageTitles = rows.map(row => header(row, "PageTitle")).map(PageTitle => (PageTitle, 1))
    val PageTitleresult = PageTitles.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("PageTitles : \n")
    PageTitleresult.foreach(row => println("( PageTitle : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    val UserRequestedDataAnalyticsInfos = rows.map(row => header(row, "UserRequestedDataAnalyticsInfo")).map(UserRequestedDataAnalyticsInfo => (UserRequestedDataAnalyticsInfo, 1))
    val UserRequestedDataAnalyticsInforesult = UserRequestedDataAnalyticsInfos.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("UserRequestedDataAnalyticsInfos : \n")
    UserRequestedDataAnalyticsInforesult.foreach(row => println("( UserRequestedDataAnalyticsInfo : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    val userstartedDataAnalyticsapplications = rows.map(row => header(row, "userstartedDataAnalyticsapplication")).map(userstartedDataAnalyticsapplication => (userstartedDataAnalyticsapplication, 1))
    val userstartedDataAnalyticsapplicationresult = userstartedDataAnalyticsapplications.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("userstartedDataAnalyticsapplications : \n")
    userstartedDataAnalyticsapplicationresult.foreach(row => println("( userstartedDataAnalyticsapplication : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    val utm_campaigns = rows.map(row => header(row, "utm_campaign")).map(utm_campaign => (utm_campaign, 1))
    val utm_campaignresult = utm_campaigns.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("utm_campaigns : \n")
    utm_campaignresult.foreach(row => println("( utm_campaign : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    val utm_contents = rows.map(row => header(row, "utm_content")).map(utm_content => (utm_content, 1))
    val utm_contentresult = utm_contents.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("utm_contents : \n")
    utm_contentresult.foreach(row => println("( utm_content : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    val utm_mediums = rows.map(row => header(row, "utm_medium")).map(utm_medium => (utm_medium, 1))
    val utm_mediumresult = utm_mediums.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("utm_mediums : \n")
    utm_mediumresult.foreach(row => println("( utm_medium : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    val utm_sources = rows.map(row => header(row, "utm_source")).map(utm_source => (utm_source, 1))
    val utm_sourceresult = utm_sources.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("utm_sources : \n")
    utm_sourceresult.foreach(row => println("( utm_source : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

    val utm_terms = rows.map(row => header(row, "utm_term")).map(utm_term => (utm_term, 1))
    val utm_termresult = utm_terms.reduceByKey(_ + _).coalesce(1).sortBy(_._2, false).take(40)
    println("utm_terms : \n")
    utm_termresult.foreach(row => println("( utm_term : " + row._1 + ",\tCount : " + row._2 + " )"))
    println("###########################################################")
    println("\n\n")

  }

}
