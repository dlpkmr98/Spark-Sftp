package com.org.dilip.pipeline.pipeline

trait Transformation[I, O] {
  def execute: I => O
}

object Transformation {
  def apply[I, O](f: I => O): Transformation[I, O] = new Transformation[I, O]  {
    override def execute: I => O = f
  }
}