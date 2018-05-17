package com.org.dilip.pipeline.executor

import scala.util.Try
import com.org.dilip.pipeline.pipeline.Staging

class SyncExecution[In, Out] extends PipelineExecutor[In, Out] {
  override def execute(in: In, stages: List[Staging])(onComplete: Try[Out] => Unit): Unit = {
    val result: Try[Out] = Try[Out](doExecute[In, Out](stages, in))

    onComplete(result)
  }

  def doExecute[I, O](stages: List[Staging], input: I): O = stages match {
    case Nil => input.asInstanceOf[O]
    case stage :: nextStage :: tail =>
      val stageOutput = stage.run(input.asInstanceOf[stage.In])

      doExecute[stage.Out, nextStage.Out](nextStage :: tail, stageOutput).asInstanceOf[O]
    case stage :: tail =>
      val stageOutput = stage.run(input.asInstanceOf[stage.In])

      doExecute[stage.Out, Out](tail, stageOutput).asInstanceOf[O]
  }
}

object SyncExecution {
  def apply[In, Out](): SyncExecution[In, Out] = new SyncExecution()
}