package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

// Compute the average number of friends by age in a social network
object FriendsByAge1 {

  // A function that splits a line of input data into (age, numFriends) into tuples
  def parseLine(line: String): (Int, Int)  = {

    //Split line by commas
    val fields = line.split(",")

    // Extract age and numFriends fields and convert to integers
    val age = fields(2).toInt
    val numFriends = fields(3).toInt

    // Create a tuple that is our result
    (age,numFriends)

  }

  // Main function where the action happens
  def main(args: Array[String]) {

    // Set the log level only to print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of local machine
    val sc = new SparkContext("local[*]","FriendsByAge")

    // Load each line of source data into an RDD
    val lines = sc.textFile("data/fakefriends-noheader.csv")

    // Use parseLine function to convert to (age,numFriends) tuple
    val rdd = lines.map(parseLine)

    // RDD of form (age, numFriends) where age is the KEY and numFriends is the VALUE
    // Use mapValues to convert each numFriends value to a tuple (numFriends, 1)
    // Use reduceByKey to sum up total numFriends and total instances for each age
    // by adding together all the numFriends and 1's respectively
    val totalsByAge= rdd.mapValues(x => (x,1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))

    // We have the tuples of (age, (totalFriends, totalInstances))
    // To compute the average we divide totalFriends / totalInstances for each age
    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)

    // Collect the results from RDD
    // This kicks off computing the DAG and actually executes the job
    val results = averagesByAge.collect()

    // Sort and print the final results
    results.sorted.foreach(println)

  }




}
