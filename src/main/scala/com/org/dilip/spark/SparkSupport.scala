package com.org.dilip.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.org.dilip.sparksftp.commons.Properties

object SparkSupport {
  
  val conf = new SparkConf().setMaster(Properties.master).setAppName(Properties.appName)
  lazy val sc = SparkContext.getOrCreate(conf)
  lazy val sqlCtx = SQLContext.getOrCreate(sc)

}