package org.miejski.recommendations.correlation

import scala.math.{pow, sqrt}

class PearsonCorrelation {


}

object PearsonCorrelation {

  def compute(user1Ratings: Seq[Double], user2Ratings: Seq[Double]): Double = {

    val user1Mean = mean(user1Ratings)
    val user2Mean = mean(user2Ratings)



    val counter = user1Ratings.zip(user2Ratings)
      .filter(s => s._1 == 0 || s._2 == 0)
      .map(s => (s._1 - user1Mean) * (s._2 - user2Mean)).sum

    counter / (singleUserDenominator(user1Ratings) * singleUserDenominator(user2Ratings))
  }

  def singleUserDenominator(userRatings: Seq[Double]) = {
    sqrt(userRatings.map(s => pow(s - mean(userRatings), 2)).sum)
  }

  private def mean(ratings: Seq[Double]): Double = {

    val realRatings = ratings.withFilter(null !=).map(s => s)
    // fuckup
    // ratings.filter(_ != null) -> nic nie wyfiltrowuje
    // ratings.filter(_ != 0.0) -> wyfiltrowuje

    val b = ratings flatMap {
      case st: Double => Some(st)
      case _ => None
    }
    realRatings.sum / realRatings.length
  }
}



