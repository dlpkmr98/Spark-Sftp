package com.org.dilip.pipeline.pipeline

import com.org.dilip.pipeline.executor.PipelineExecutor
import scala.util.Try
import com.org.dilip.pipeline.executor.SyncExecution

trait Pipeline[Input, Output] {
  val stages: List[Staging]

  def >>[X](f: Output => X): Pipeline[Input, X] = add(f)

  def add[X](f: Output => X): Pipeline[Input, X] = {
    val stage = Staging[Output, X](Transformation[Output, X](f))

    Pipeline[Input, X](stage :: stages)
  }

  def >>[X](filter: Transformation[Output, X]): Pipeline[Input, X] = add(filter)

  def add[X](filter: Transformation[Output, X]): Pipeline[Input, X] = {
    val stage = Staging[Output, X](filter)

    Pipeline[Input, X](stage :: stages)
  }

  def start(in: Input)(onComplete: Try[Output] => Unit)(implicit pipelineExecutor: PipelineExecutor[Input, Output]): Unit = {
    pipelineExecutor.execute(in, stages.reverse)(onComplete)
  }
}

object Pipeline {
  def apply[Input, Output](stagesList: List[Staging]): Pipeline[Input, Output] = new Pipeline[Input, Output] {
    override val stages: List[Staging] = stagesList
  }

  def apply[Input, Output](): Pipeline[Input, Output] = Pipeline[Input, Output](List())
}