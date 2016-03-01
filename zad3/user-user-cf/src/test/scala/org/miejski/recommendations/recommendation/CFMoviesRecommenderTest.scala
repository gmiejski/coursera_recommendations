package org.miejski.recommendations.recommendation

import org.miejski.recommendations.model.UserRating
import org.miejski.recommendations.neighbours.{NeighbourInfo, UserAverageRating}
import org.scalatest.{FunSuite, Matchers}

import scala.util.Random

class CFMoviesRecommenderTest extends FunSuite
  with Matchers {

  test("predicting users rating when only single user matters") {
    // coursera-dataset user = 3712 neighbours
    val neighbours = Seq(("2824", 0.46291), ("3867", 0.400275), ("5062", 0.247693), ("442", 0.22713), ("3853", 0.19366))
      .map(s => NeighbourInfo(s._1, s._2, Random.nextDouble() * 5.0, Random.nextDouble() * 5.0))
    val neighboursRatings = Seq(UserRating("2824", Option.empty),
      UserRating("3867", Option.empty),
      UserRating("5062", Option.empty),
      UserRating("442", Option(5.0)),
      UserRating("3853", Option.empty))

    // coursera-dataset movie(id = 641) recommendation
    val rating = CFMoviesRecommender.standardPrediction(UserAverageRating("u1", 3.0), neighbours, neighboursRatings)

    rating.get should equal(5.0)
  }

  test("should return proper value when all users matters") {
    val neighbours = Seq(("1", 0.5), ("2", 1.0), ("3", 0.5), ("4", 0.5))
      .map(s => NeighbourInfo(s._1, s._2, Random.nextDouble() * 5.0, Random.nextDouble() * 5.0))
    val neighboursRatings = Seq(
      UserRating("1", Option(3.0)),
      UserRating("2", Option(4.0)),
      UserRating("3", Option(3.0)),
      UserRating("4", Option.empty))

    val rating = CFMoviesRecommender.standardPrediction(UserAverageRating("u1", 3.0), neighbours, neighboursRatings)

    rating.get should equal(3.5)
  }
}
