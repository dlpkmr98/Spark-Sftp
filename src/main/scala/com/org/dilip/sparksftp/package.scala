package com.org.dilip.sparksftp

import com.org.dilip.pipeline.pipeline.Pipeline
import com.org.dilip.pipeline.executor.PipelineExecutor
import com.org.dilip.pipeline.executor.SyncExecution
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame

package object sparksftp {
  type RorD = Either[RDD[Row], DataFrame]

  // from RorD to object
  implicit def i1(rd: RorD): DataFrame = rd.right.get
  implicit def i2(rd: RorD): RDD[Row] = rd.left.get
  // from object to RorD
  implicit def i3(df: DataFrame): RorD = Right(df)
  implicit def i4(rdd: RDD[Row]): RorD = Left(rdd)
  // from object to option of RorD
  implicit def i5(df: DataFrame): Option[RorD] = Some(Right(df))
  implicit def i6(rdd: RDD[Row]): Option[RorD] = Some(Left(rdd))
  

}
