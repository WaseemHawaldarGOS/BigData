package com.simplilearn.video4

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Input data
  *
  * S.No  Customer Name Location  Project_name  no.ofSoldUnits  Date
  * 23262	Candice Levy	Congo	SUPA101	Retail	117	2012-08-09
  * 23263	Xerxes Smith	Panama	DETA200	Retail	73	2012-07-06
  * 23264	Levi Douglas	Tanzania, United Republic of	DETA800	Retail	205	2012-08-18
  * 23265	Uriel Benton	South Africa	SUPA104	Online	14	2012-08-05
  * 23266	Celeste Pugh	Gabon	PURA200	Online	170	2012-08-11
  * 23267	Vance Campos	Syrian Arab Republic	PURA100	Direct	129	2012-07-11
  * 23268	Latifah Wall	Guadeloupe	DETA100	Retail	82	2012-07-12
  * 23269	Jane Hernandez	Macedonia	PURA100	Retail	116	2012-06-03
  * 23270	Wanda Garza	Kyrgyzstan	SUPA103	Online	67	2012-06-07
  * 23271	Athena Fitzpatrick	Reunion	SUPA103	Online	125	2012-07-27
  * 23272	Anjolie Hicks	Turks and Caicos Islands	DETA200	Online	71	2012-07-31
  * 23273	Isaac Cooper	Netherlands Antilles	SUPA104	Direct	22	2012-08-13
  * 23274	Asher Weber	Macedonia	PURA100	Online	153	2012-08-22
  * 23275	Ethan Gregory	Tuvalu	DETA800	Retail	141	2012-07-04
  * 23276	Hayes Rollins	Nepal	PURA500	Online	65	2012-08-01
  * 23277	MacKenzie Moss	Oman	SUPA101	Online	157	2012-07-12
  *
  *
  * Find the total number of units sold for every project.
  *
  */
object SalesAnalysis {

  def main(args: Array[String]): Unit = {
    /*
      setMaster tells were spark is running. In case of cluster setMaster should be set to yarn.
      For the sake of simplicity keep both object name and appname as same.
     */
    val sc = new SparkContext(new SparkConf().setAppName("SalesAnalysis").setMaster("local[2]"))
    val salesData = "D://JPMC-Data//SimplyLearn//samplefiles//smallSalesData"
    val lines = sc.textFile(salesData)
    val linesSplitted = lines.map(line => line.split("\t"))
    val projectAndPriceTuple = linesSplitted.map(field => (field(3), field(5).toInt))
    //Note: If field(5).toInt is not done it will concatenate sales number instead of adding it.
    println(projectAndPriceTuple.collect().mkString(","))
    /*
    (SUPA101,117),(DETA200,73),(DETA800,205),(SUPA104,14),(PURA200,170),(PURA100,129),(DETA100,82),(PURA100,116),(SUPA103,67),(SUPA103,125),(DETA200,71),(SUPA104,22),(PURA100,153),(DETA800,141),(PURA500,65),(SUPA101,157)
     */
    val resultantSum = projectAndPriceTuple.reduceByKey(_ + _).collect()
    println(resultantSum.mkString(", "))
   /*
   output - (SUPA104,36), (DETA800,346), (DETA200,144), (PURA200,170), (PURA100,398), (SUPA101,274), (PURA500,65), (DETA100,82), (SUPA103,192)
    */
  }

}
