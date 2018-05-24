package com.org.dilip.sparksftp

import com.org.dilip.pipeline.pipeline.Pipeline
import com.org.dilip.sparksftp.commons.Properties
import com.org.dilip.sparksftp.jobs._
import com.org.dilip.sparksftp.jobs.SftpMultiFileWriter
import scala.util.Success
import scala.util.Failure
import com.org.dilip.pipeline.executor.PipelineExecutor
import com.org.dilip.pipeline.executor.SyncExecution
import scala.util.Try

trait MainStreamService {
  //required implicit declaration
  implicit def >>[A](c: A) = c.asInstanceOf[Pipeline[Seq[String], Some[String]]]
  implicit def >>>[C](c: C) = c.toString()
}

class MainStream extends MainStreamService {
  def init[A, B <: Array[String], C <: String](f:(A,C) => Unit,f1: => A, f2: (B, C) => C) = (x: B) => (y: C) => f(f1,f2(x, y))
}

object MainStream {
  def apply() = new MainStream
  //way to add stages in pipeline
  //change read/write format ex: MultiFileReader(Formatter.apply("parquet")), default format is csv
  def createPipline = Pipeline[Seq[String], Seq[String]]() >> MultiFileReader(Formatter.apply()) >>
    MultiFileWriter(Formatter.apply()) >> SftpMultiFileWriter()
    
  def getInput(args: Array[String], in: String): String = {
    Try(Option(args(0)).getOrElse(in)) match {
      case Failure(ex) => in
      case Success(s) => s.isEmpty() match {
        case true  => in
        case false => s
      }
    }
  }
  //use AsyncExecution to switch to parallel execution
  implicit val pipelineExecutor: PipelineExecutor[Seq[String], Some[String]] = SyncExecution()
  def executePipline(pl: Pipeline[Seq[String], Some[String]],in:String)= pl.start(in.split(",").toSeq) {
    case Success(s)  => println(s"$s")
    case Failure(ex) => ex.printStackTrace()
  }
  lazy val in = Properties.input_path
  //functional way
  def main(args: Array[String]): Unit = MainStream().init(executePipline,createPipline, getInput)(args)(in)
}
