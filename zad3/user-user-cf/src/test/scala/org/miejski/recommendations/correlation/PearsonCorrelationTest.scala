package org.miejski.recommendations.correlation

import org.miejski.recommendations.helper.ShortCaseClasses
import org.scalatest.{FunSuite, Matchers}

class PearsonCorrelationTest extends FunSuite
  with Matchers
  with ShortCaseClasses {


  test("should properly count correlation") {

    val first = Seq(null, 2.0, 5.0, 7.0, 1.0, 8.0)
    val second = Seq(2.0, 4.0, 3.0, 6.0, 1.0, 5.0)

    PearsonCorrelation.compute(toOptional(first), toOptional(second)) shouldEqual NeighboursDetails(0.793, 4.6, 3.8, 4.6, 3.5)
  }

  test("should properly compute 2 same dataset") {
    val data = Seq(2.0, 5.0, 7.0, 1.0, 8.0).map(Option(_))
    PearsonCorrelation.compute(data, data) shouldEqual NeighboursDetails(1.0, 4.6, 4.6, 4.6, 4.6)
  }

  test("should say -1 at different data") {
    val first = Seq(2.0, 0.0).map(Option(_))
    val second = Seq(-1.0, 6.0).map(Option(_))

    PearsonCorrelation.compute(first, second) shouldEqual NeighboursDetails(-1.0, 1.0, 2.5, 1.0, 2.5)
  }

  test("should properly compute correlation") {
    val first = List(mr("2", 5.0), mr("3", 2.0), mr("4", 8.0), mr("5", 7.0), mr("6", 1.0))
    //    val first = Seq(null, 2.0, 5.0, 7.0, 1.0, 8.0)
    //    val second = Seq(2.0, 4.0, 3.0, 6.0, 1.0, 5.0)
    val second = List(mr("1", 2.0), mr("2", 3.0), mr("3", 4.0), mr("4", 5.0), mr("5", 6.0), mr("6", 1.0))

    PearsonCorrelation.compute(first, second) shouldEqual NeighboursDetails(0.793, 4.6, 3.8, 4.6, 3.5)
  }

  test("should properly compute 2 same dataset correlation") {
    val data = List(mr("1", 2.0), mr("23", 5.0), mr("8", 8.0), mr("22", 1.0), mr("7", 7.0))
    PearsonCorrelation.compute(data, data) shouldEqual NeighboursDetails(1.0, 4.6, 4.6, 4.6, 4.6)
  }

  test("should say -1 at different data correlation") {
    val first = List(mr("1", 2.0), mr("2", 0.0))
    val second = List(mr("1", -1.0), mr("2", 6.0))

    PearsonCorrelation.compute(first, second) shouldEqual NeighboursDetails(-1.0, 1.0, 2.5, 1.0, 2.5)
  }

  def toOptional[T](a: Seq[T]): Seq[Option[Double]] = {
    a.collect {
      case null => None
      case x: Double => Some(x)
    }
  }


}
