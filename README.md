################################################################################################################################################

                                                               **Readme file**

################################################################################################################################################

Export environent variable :
export WEB_ANALYTICS=/home/akash/web_analytics/

################################################################################################################################################


* directory structure.**

W|-- input
|   `-- input.csv
|-- output
|   |-- AllParameters.out
|   |-- AnalyticsCorrelated.out
|   |-- CityWise.out
|   |-- CourseWise.out
|   |-- IndependentAnalytics.out
|   |-- OperatingSystemWise.out
|   `-- RegionWise.out
|-- pom.xml
|-- README.md
`-- src
    |-- main
    |   `-- scala
    |       `-- com
    |           `-- organization
    |               |-- Analytics
    |               |   |-- AnalyticsAllPar.scala
    |               |   |-- AnalyticsCity.scala
    |               |   |-- AnalyticsCorrelated.scala
    |               |   |-- AnalyticsCourse.scala
    |               |   |-- AnalyticsIndependent.scala
    |               |   |-- AnalyticsOS.scala
    |               |   |-- AnalyticsRegion.scala
    |               |   `-- Analytics.scala
    |               |-- Context
    |               |   `-- Context.scala
    |               |-- Helpers
    |               |   |-- CLIArguments.scala
    |               |   `-- CSVHeader.scala
    |               `-- main.scala
    `-- test



###############################################################################################################################################

Steps to run the jobs:

**Note: env variable WEB_ANALYTICS and SPARK_HOME should be exported.**

1.) Build : 	a.) mv ${WEB_ANALYTICS}/
		b.) mvn clean install


2.) Run jobs: 	spark-submit --class <main-class> --master <master-url> --deploy-mode <deploy-mode> <application-jar> <SPARK_JOB_NAME>
 <PARTITIONS> <SPARK_CONTEXT_MODE>
 

	a.) IndependentAnalytics :
			spark-submit --class com.organization.main ${WEB_ANALYTICS}/target/original-analytics-1.0-SNAPSHOT.jar IndependentAnalytics 4 LOCAL > ${WEB_ANALYTICS}/output/IndependentAnalytics.out

	b.) CityWise :
			spark-submit --class com.organization.main ${WEB_ANALYTICS}/target/original-analytics-1.0-SNAPSHOT.jar CityWise 4 LOCAL > ${WEB_ANALYTICS}/output/CityWise.out

	c.) OperatingSystemWise :
			spark-submit --class com.organization.main ${WEB_ANALYTICS}/target/original-analytics-1.0-SNAPSHOT.jar OperatingSystemWise 4 LOCAL > ${WEB_ANALYTICS}/output/OperatingSystemWise.out

	d.) RegionWise : 
			spark-submit --class com.organization.main ${WEB_ANALYTICS}/target/original-analytics-1.0-SNAPSHOT.jar RegionWise 4 LOCAL > ${WEB_ANALYTICS}/output/RegionWise.out

	e.) CourseWise :
			spark-submit --class com.organization.main ${WEB_ANALYTICS}/target/original-analytics-1.0-SNAPSHOT.jar CourseWise 4 LOCAL > ${WEB_ANALYTICS}/output/CourseWise.out


	f.) AllParameters :
			spark-submit --class com.organization.main ${WEB_ANALYTICS}/target/original-analytics-1.0-SNAPSHOT.jar AllParameters 4 LOCAL > ${WEB_ANALYTICS}/output/AllParameters.out	


	g.) Correlated :
			spark-submit --class com.organization.main ${WEB_ANALYTICS}/target/original-analytics-1.0-SNAPSHOT.jar AnalyticsCorrelated 4 LOCAL > ${WEB_ANALYTICS}/output/AnalyticsCorrelated.out
	
################################################################################################################################################		

Output directory: ${WEB_ANALYTICS}/output/

################################################################################################################################################



























