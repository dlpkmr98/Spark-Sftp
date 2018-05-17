package com.org.dilip.sparksftp.jobs

import com.org.dilip.pipeline.pipeline.Transformation
import org.apache.spark.sql.DataFrame
import com.org.dilip.sparksftp.commons.IO
import com.org.dilip.sparksftp.commons.Properties



class MultiFileWriter[W <: Formatter](w: W) extends Transformation[Seq[DataFrame], Seq[DataFrame]] with IO  {

  /* (non-Javadoc)
 * @see com.org.dilip.pipeline.pipeline.Transformation#execute()
 */
override def execute = x => {
    for (ind <- x.indices ) yield {
       w.format match {
        case "csv" => writeDataToCsv(x (ind),ind+Properties.file_name,Properties.output_path,w.sep,w.mode)
        case "parquet" => toWriteParquet(x (ind),ind+Properties.file_name,Properties.output_path,w.mode)
        case _ => throw new RuntimeException("Only supported format are: csv,parquet")
      }
        
    }
   x
}
}

object MultiFileWriter {
  def apply[W <: Formatter](x: W): MultiFileWriter[W] = new MultiFileWriter(x)
}