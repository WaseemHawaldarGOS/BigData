package com.simplilearn.video4

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Input data
  * ERROR:	php	21/05/2015
  * DONE:	php	11/01/2016
  * ERROR:	RailsApp	05/08/2015
  * ERROR:	php	19/05/2015
  * DONE:	mysql	23/01/2016
  * ERROR:	php	13/02/2016
  * ERROR:	php	22/11/2014
  * ERROR:	RailsApp	25/12/2015
  * ERROR:	mysql	18/03/2015
  * DONE:	php	22/08/2015
  * ERROR:	RailsApp	06/05/2015
  * ERROR:	php	09/03/2015
  * DONE:	mysql	28/06/2015
  *
  *
  * Given file -> D:\JPMC-Data\SimplyLearn\samplefiles\server_log
  *
  * Find below use cases:
  * number of errors
  * number of php errors
  * how many php/mysql errors happen between 2 dates
  *
  */



object ServerLogAnalysis {

  def main(args: Array[String]): Unit = {
  /*
    setMaster tells were spark is running. In case of cluster setMaster should be set to yarn.
    For the sake of simplicity keep both object name and appname as same.
   */
    val sc = new SparkContext(new SparkConf().setAppName("ServerLogAnalysis").setMaster("local[2]"))
    val logFile = "D://JPMC-Data//SimplyLearn//samplefiles//server_log"
    val lines = sc.textFile(logFile)
    val errors = lines.filter(_.startsWith("ERROR"))
    val messages = errors.map(_.split("\t")).map(r => r(1))
    messages.cache()
    val tot = lines.count()
    val mysql = messages.filter(_.contains("mysql")).count()
    val php = messages.filter(_.contains("php")).count()
    val rail = messages.filter(_.contains("RailsApp")).count()

    println("Total msgs: %s, MYSQL errs: %s, PHP errs: %s, RAILS errs: %s, DONE errs: %s".format(tot, mysql,php,rail,(tot - (mysql+php+rail))))
  }
}
