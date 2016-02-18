package org.miejski.recommendations

import org.apache.spark.sql.Row
import org.miejski.recommendations.parser.RatingsReader
import org.scalatest.{FunSuite, Matchers}

class MainAppTest extends FunSuite
  with Matchers {


  test("should parse ratings") {
    val first = Seq("userId", null, "2.0", "5.5", "7.0", "1.0", "8.0")

    val row = Row("userId", null, "2.0", "5.5", "7.0", "1.0", "8.0")

    val parsedRow = RatingsReader.parseRatings(row)
    parsedRow._1 shouldBe "userId"
    parsedRow._2 shouldBe first.drop(1).map(RatingsReader.toOptionalRating)
  }

  test("should something") {

    val second = Seq(2.0, 4.0, 3.0, 6.0, 1.0, 5.0)

  }

}
