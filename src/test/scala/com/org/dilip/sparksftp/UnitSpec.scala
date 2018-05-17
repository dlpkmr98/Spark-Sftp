package com.org.dilip.sparksftp

import org.scalatest._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.DataFrame
import com.org.dilip.spark.SparkSupport


abstract class UnitSpec extends FlatSpec with Matchers  with UnitSpecTrait

trait UnitSpecTrait extends SuiteMixin with BeforeAndAfterAll {
  this: Suite =>

  // Sharing immutable fixture objects via instance variables
  val sqlCtx = SparkSupport.sqlCtx
  import sqlCtx.implicits._
  
  override def beforeAll {
    setSparkContextParameter
  }

  override def afterAll {
    sqlCtx.clearCache()
  }

  def setSparkContextParameter = {
    sqlCtx.setConf("spark.sql.broadcastTimeout", "1200")
    sqlCtx.setConf("spark.default.parallelism", "10")
    sqlCtx.setConf("spark.sql.shuffle.partitions", "10")
    sqlCtx.setConf("spark.shuffle.consolidateFiles", "true")
    sqlCtx.setConf("spark.driver.extraJavaOptions", "-Xmx5g")
    Logger.getRootLogger().setLevel(Level.ERROR)

  }
  
  val dfWithNegValue: DataFrame = {
    Seq(("1", "2", "3", "4"), ("11", "-22", "33", "44")).toDF("c1", "c2", "c3", "c4")
  }
  
}