package com.org.dilip.sparksftp.jobs

import com.org.dilip.sparksftp.commons.IO
import com.org.dilip.pipeline.pipeline.Transformation
import org.apache.spark.sql.DataFrame
import com.org.dilip.sparksftp.sparksftp._

case class Formatter (format:String = "csv", sep:String =",",mode:String = "overwrite")

class MultiFileReader[R<:Formatter](r: R) extends Transformation[Seq[String], Seq[Option[RorD]]] with IO {

  /* (non-Javadoc)
 * @see com.org.dilip.pipeline.pipeline.Transformation#execute()
 */
  override def execute = x => for (path <- x) yield {
     r.format match {
       case "csv" => convert(toReadDataFromCsv(path))
       case "parquet" => convert(toReadDataFromParquet(path))
       case _ => throw new RuntimeException("Only supported format are: csv,parquet")
     }
  }
}

object MultiFileReader {
  def apply[R <: Formatter](x: R):MultiFileReader[R] = new MultiFileReader(x)
}
