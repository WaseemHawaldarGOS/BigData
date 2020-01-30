package com.simplilearn.video4

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]) {
    /*
    Actual file
     */
    //val wordFile = "file:///D:/JPMC-Data/SimplyLearn/samplefiles/wordcountproblem"

    /*
    Sample file to understand flow
     */
    val wordFile = "file:///D:/JPMC-Data/SimplyLearn/samplefiles/smallwordcountdata.txt"
    val conf = new SparkConf().setAppName("Word Count Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val threshold = 20
    val tokenized = sc.textFile(wordFile).flatMap(_.split(" "))
    val wordCounts = tokenized.map((_, 1))
    println(wordCounts.collect().mkString(","))
    /*
    (Big,1),(Data,1),(Big,1),(Data,1),(Big,1),(Data,1),(Big,1),(Data,1),(Big,1),(Data,1),(Big,1),(Data,1),(Big,1),(Data,1),(Big,1),(Data,1),(Big,1),(Data,1),(Big,1),(Data,1),(Big,1),(Data,1),(Big,1),(Data,1),(Big,1),(Data,1),(Big,1),(Data,1),(Big,1),(Data,1),(Big,1),(Data,1),(Big,1),(Data,1),(Big,1),(Data,1),(Big,1),(Data,1),(Big,1),(Data,1)
     */
      val aftreSummation = wordCounts.reduceByKey(_ + _)
    print(aftreSummation.collect().mkString(", "))
    /*
    output - (Big,20), (Data,20)
     */

    //Below line picks second elements of tuples. In our case for Big it will pick 20.
    val filtered = wordCounts.filter(_._2 >= threshold)

    //Below line picks first elements of tuples and convert them to char array. Finally finds sum of characters
    val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)
    print(charCounts.collect().mkString(", "))
    /*
    output - (B,1), (t,1), (D,1), (a,2), (i,1), (g,1)
     */
    sc.stop
  }

}
