package com.org.dilip.sparksftp.jobs

import com.org.dilip.pipeline.pipeline.Transformation
import org.apache.spark.sql.DataFrame
import com.org.dilip.sparksftp.commons.IO
import com.org.dilip.sparksftp.commons.Properties
import com.org.dilip.pipeline.pipeline.Staging

class FileReader extends Transformation[String, DataFrame] with IO {

  override def execute: String => DataFrame = x => toReadDataFromCsv(x)
}

object FileReader {
  def apply(): FileReader = new FileReader()
}


