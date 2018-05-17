package com.org.dilip.sparksftp

import com.org.dilip.pipeline.pipeline.Pipeline
import com.org.dilip.sparksftp.commons.Properties
import com.org.dilip.sparksftp.jobs._
import com.org.dilip.sparksftp.jobs.SftpMultiFileWriter
import scala.util.Success
import scala.util.Failure
import com.org.dilip.pipeline.executor.PipelineExecutor
import com.org.dilip.pipeline.executor.SyncExecution

trait MainStreamService {
  implicit def >>[A](c: A) = c.asInstanceOf[Pipeline[Seq[String], Some[String]]]
  //use AsyncExecution to switch to parallel execution
  implicit val pipelineExecutor: PipelineExecutor[Seq[String], Some[String]] = SyncExecution()
  
  def executePipline(pl: Pipeline[Seq[String], Some[String]]) = pl.start(Properties.input_path.split(",").toSeq) {
    case Success(s)  => println(s"$s")
    case Failure(ex) => ex.printStackTrace()
  }
}

class MainStream extends MainStreamService {
  //way to add stages in pipeline
  //change read/write format ex: MultiFileReader(Formatter.apply("parquet")), default format is csv
  def createPipline =
    Pipeline[Seq[String], Seq[String]]() >>
      MultiFileReader(Formatter.apply("abc")) >>
      MultiFileWriter(Formatter.apply()) >>
      SftpMultiFileWriter()
  def init[A <: Pipeline[Seq[String], Some[String]]](f: => A) = executePipline(f)

}

object MainStream {
  def apply() = new MainStream
  val pipeline = MainStream() createPipline
  def main(args: Array[String]): Unit = MainStream() init pipeline
}





