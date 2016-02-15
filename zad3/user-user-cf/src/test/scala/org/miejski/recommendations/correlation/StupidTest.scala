package org.miejski.recommendations.correlation

import org.scalatest.{FunSuite, Matchers}

/**
  * Created by grzegorz.miejski on 11/02/16.
  */
class StupidTest extends FunSuite with Matchers {

  test("asd") {

    val first: Seq[Double] = Seq(null, 2.0, 5.0, 7.0, 1.0, 8.0).asInstanceOf[Seq[Double]]
    val second = Seq(2.0, 4.0, 3.0, 6.0, 1.0, 5.0)

    val a = first.filter(_ != null)
    val b = first.withFilter(null !=).map(s => s)

    println("dupa")
  }
  //  def fukinFilter(double1: Double, double2: Double) = double1 != double2
}
