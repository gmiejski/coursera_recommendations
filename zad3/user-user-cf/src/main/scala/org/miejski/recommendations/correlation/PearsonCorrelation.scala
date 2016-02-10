package org.miejski.recommendations.correlation

import scala.math.{pow, sqrt}

class PearsonCorrelation {


}

object PearsonCorrelation {

  def compute(user1Ratings: Seq[Option[Double]], user2Ratings: Seq[Option[Double]]): Double = {

    val filtered: Seq[(Double, Double)] = user1Ratings.zip(user2Ratings)
      .collect { case (Some(a), Some(b)) => (a, b) }

    val user1SharedRatings = filtered.map(_._1)
    val user2SharedRatings = filtered.map(_._2)

    val user1Mean = mean(user1SharedRatings)
    val user2Mean = mean(user2SharedRatings)

    val counter = filtered
      .map(s => (s._1 - user1Mean) * (s._2 - user2Mean)).sum

    val correlation = counter / (singleUserDenominator(user1SharedRatings, user1Mean) * singleUserDenominator(user2SharedRatings, user2Mean))
    "%.4f".format(correlation).toDouble
  }

  def singleUserDenominator(userRatings: Seq[Double], mean: Double) = {
    sqrt(userRatings.map(s => pow(s - mean, 2)).sum)
  }

  private def mean(ratings: Seq[Double]): Double = {
    ratings.sum / ratings.length
  }

  private def meanOptional(ratings: Seq[Option[Double]]): Double = {

    val realRatings = ratings.flatten
    // fuckup
    // ratings.filter(_ != null) -> nic nie wyfiltrowuje
    // ratings.filter(_ != 0.0) -> wyfiltrowuje

    realRatings.sum / realRatings.length
  }
}



