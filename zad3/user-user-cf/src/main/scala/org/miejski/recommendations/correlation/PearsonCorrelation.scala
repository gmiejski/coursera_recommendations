package org.miejski.recommendations.correlation

import org.miejski.recommendations.evaluation.model.MovieRating
import org.miejski.recommendations.parser.DoubleFormatter

import scala.math.{pow, sqrt}

class PearsonCorrelation {


}

object PearsonCorrelation extends DoubleFormatter {

  def compute(user1Ratings: List[MovieRating], user2Ratings: List[MovieRating]): NeighboursDetails = {
    val filtered = (user1Ratings.map(s => (s.movie.id, s.rating)) ++ user2Ratings.map(s => (s.movie.id, s.rating)))
      .groupBy(_._1)
      .filter { case kv => kv._2.length == 2 }
      .values
      .map(s => (s.apply(0)._2.get, s.apply(1)._2.get))
      .toSeq

    val user1SharedRatings = filtered.map(_._1)
    val user2SharedRatings = filtered.map(_._2)

    val user1SharedMean = mean(user1SharedRatings)
    val user2SharedMean = mean(user2SharedRatings)

    val user1TotalMean = mean(user1Ratings.map(s => s.rating.get))
    val user2TotalMean = mean(user2Ratings.map(s => s.rating.get))

    val counter = filtered
      .map(s => (s._1 - user1SharedMean) * (s._2 - user2SharedMean)).sum

    val correlation = counter / (singleUserDenominator(user1SharedRatings, user1SharedMean) * singleUserDenominator(user2SharedRatings, user2SharedMean))
    NeighboursDetails(format(correlation), format(user1SharedMean), format(user2SharedMean), user1TotalMean, user2TotalMean)
  }

  def compute(user1Ratings: Seq[Option[Double]], user2Ratings: Seq[Option[Double]]): NeighboursDetails = {

    val filtered: Seq[(Double, Double)] = user1Ratings.zip(user2Ratings)
      .collect { case (Some(a), Some(b)) => (a, b) }

    val user1SharedRatings = filtered.map(_._1)
    val user2SharedRatings = filtered.map(_._2)

    val user1SharedMean = mean(user1SharedRatings)
    val user2SharedMean = mean(user2SharedRatings)

    val user1TotalMean = mean(user1Ratings.collect { case Some(a) => a })
    val user2TotalMean = mean(user2Ratings.collect { case Some(a) => a })

    val counter = filtered
      .map(s => (s._1 - user1SharedMean) * (s._2 - user2SharedMean)).sum

    val correlation = counter / (singleUserDenominator(user1SharedRatings, user1SharedMean) * singleUserDenominator(user2SharedRatings, user2SharedMean))
    NeighboursDetails(format(correlation), format(user1SharedMean), format(user2SharedMean), user1TotalMean, user2TotalMean)
  }

  def singleUserDenominator(userRatings: Seq[Double], mean: Double) = {
    sqrt(userRatings.map(s => pow(s - mean, 2)).sum)
  }

  private def mean(ratings: Seq[Double]): Double = {
    ratings.sum / ratings.length
  }
}

case class NeighboursDetails(similarity: Double,
                             user1SharedAverageRating: Double,
                             user2SharedAverageRating: Double,
                             user1AverageRating: Double,
                             user2AverageRating: Double)

