package org.miejski.recommendations.correlation

import scala.math.{pow, sqrt}

class PearsonCorrelation {


}

object PearsonCorrelation {

  def compute(user1Ratings: Seq[Option[Double]], user2Ratings: Seq[Option[Double]]): Double = {

    val user1Mean = mean(user1Ratings)
    val user2Mean = mean(user2Ratings)
    val filtered: Seq[(Double, Double)] = user1Ratings.zip(user2Ratings)
      .collect { case (Some(a), Some(b)) => (a, b) }
    //      .filter(s => s._1 != 0.0 && s._2 != 0.0)
    val counter = filtered
      .map(s => (s._1 - user1Mean) * (s._2 - user2Mean)).sum

    def correlation = counter / (singleUserDenominator(user1Ratings) * singleUserDenominator(user2Ratings))
    "%.4f".format(correlation).toDouble
  }

  def singleUserDenominator(userRatings: Seq[Option[Double]]) = {
    val collect: Seq[Double] = userRatings.collect { case Some(s) => s }
    sqrt(collect.map(s => pow(s - mean(userRatings), 2)).sum)
  }

  private def mean(ratings: Seq[Option[Double]]): Double = {

    val realRatings = ratings.collect { case Some(s) => s }
    // fuckup
    // ratings.filter(_ != null) -> nic nie wyfiltrowuje
    // ratings.filter(_ != 0.0) -> wyfiltrowuje

    realRatings.sum / realRatings.length
  }
}



