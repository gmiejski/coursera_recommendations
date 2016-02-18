package org.miejski.recommendations.parser

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.miejski.recommendations.model.UserRating
import org.scalatest.{FunSuite, Matchers}

class RatingsReaderTest extends FunSuite
  with Matchers {

  test("parsing csv movie row file") {

    val sparkConfig = new SparkConf().setAppName("UserUserCollaborativeFiltering").setMaster("local[2]")
    val sc = SparkContext.getOrCreate(sparkConfig)

    val ratings = RatingsReader.readRatings("src/test/resources/test-movie-row.csv", SQLContext.getOrCreate(sc))

    ratings.count() shouldEqual 4
    ratings.filter(_._1.id.equals("641")).map(_._2).first() should contain allOf(
      UserRating("442", Option(5.0)),
      UserRating("2756", Option.empty),
      UserRating("860", Option(3.0)))
  }
}
