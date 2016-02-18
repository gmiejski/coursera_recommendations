package org.miejski.recommendations.model

import org.scalatest.{FunSuite, Matchers}

class MovieTest extends FunSuite
  with Matchers {

  test("should properly retrieve movie info") {

    val movieWithId = "120: The Lord of the Rings: The Fellowship of the Ring (2001)"

    val movie = Movie(movieWithId)

    movie shouldEqual Movie("120", "The Lord of the Rings: The Fellowship of the Ring (2001)")
  }
}
