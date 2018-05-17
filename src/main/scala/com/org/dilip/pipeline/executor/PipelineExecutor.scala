package com.org.dilip.pipeline.executor

import scala.util.Try
import com.org.dilip.pipeline.pipeline.Staging

trait PipelineExecutor[In, Out] {
  def execute(in: In, stages: List[Staging]) (onComplete: Try[Out] => Unit) : Unit
}