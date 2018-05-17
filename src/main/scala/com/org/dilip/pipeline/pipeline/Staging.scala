package com.org.dilip.pipeline.pipeline

trait Staging {
  type In
  type Out

  val filter: Transformation[In, Out]

  def run(in: In): Out = filter.execute(in)
}

object Staging {
  def apply[I, O](f: Transformation[I, O]): Staging = new Staging() {
    override type In = I
    override type Out = O
    override val filter: Transformation[In, Out] = f
  }
}