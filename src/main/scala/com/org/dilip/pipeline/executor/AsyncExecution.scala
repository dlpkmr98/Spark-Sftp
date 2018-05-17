package com.org.dilip.pipeline.executor

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.org.dilip.pipeline.pipeline.Staging
import scala.util.Try

class AsyncExecution[In, Out] (implicit executor: ExecutionContext) extends PipelineExecutor[In, Out] {
  override def execute(in: In, stages: List[Staging])(onComplete: Try[Out] => Unit): Unit = {
    val out: Future[Out] = doExecute[In, Out](stages, Future.successful[In](in))

    out.onComplete(onComplete)
  }

  def doExecute[I, O](stages: List[Staging], input: Future[I]): Future[O] = stages match {
    case Nil => input.asInstanceOf[Future[O]]
    case stage :: nextStage :: tail =>
      val stageOutput = input.map[stage.Out](i => stage.run(i.asInstanceOf[stage.In]))

      doExecute[stage.Out, nextStage.Out](nextStage :: tail, stageOutput).asInstanceOf[Future[O]]
    case stage :: tail =>
      val stageOutput = input.map[stage.Out](i => stage.run(i.asInstanceOf[stage.In]))

      doExecute[stage.Out, Out](tail, stageOutput).asInstanceOf[Future[O]]
  }
}

object AsyncExecution {
  def apply[In, Out]: AsyncExecution[In, Out] = new AsyncExecution() (scala.concurrent.ExecutionContext.Implicits.global)
}