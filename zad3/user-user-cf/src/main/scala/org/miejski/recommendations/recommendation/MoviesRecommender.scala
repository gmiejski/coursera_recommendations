package org.miejski.recommendations.recommendation

import org.apache.spark.rdd.RDD
import org.miejski.recommendations.model.{Movie, UserRating}
import org.miejski.recommendations.neighbours.{NeighbourInfo, Neighbours, UserAverageRating}
import org.miejski.recommendations.parser.DoubleFormatter

class MoviesRecommender(neighbours: Neighbours,
                        moviesRatings: RDD[(Movie, Seq[UserRating])],
                        predictionMethod: (UserAverageRating, Seq[NeighbourInfo], Seq[UserRating]) => Option[Double]) extends Serializable
  with DoubleFormatter {

  def forUser(user: String, top: Int = 0): Seq[(Movie, Double)] = {
    val closestNeighbours: Seq[NeighbourInfo] = neighbours.findFor(user)
    val closestNeighboursIds = closestNeighbours.map(_.neighbourName)

    val userAverageRating = neighbours.getUserAverageRating(user)

    val neighboursRatings = moviesRatings.map(mRating => (mRating._1, mRating._2.filter(userRating => closestNeighboursIds.contains(userRating.user))))
    val predictedRatings = neighboursRatings
      .map(nr => (nr._1, predictionMethod(userAverageRating, closestNeighbours, nr._2))).collect()

    val moviesSortedByPredictedRating = predictedRatings.filter(_._2.isDefined)
      .map(s => (s._1, s._2.get))
      .sortBy(s => s._2)
      .map(prediction => (prediction._1, format(prediction._2)))
      .reverse
    if (top <= 0) moviesSortedByPredictedRating else moviesSortedByPredictedRating.take(top)
  }

  def toNeighboursRatingWithSimiliarity(movies: RDD[Movie], closestNeighbours: RDD[(String, Double)], s: (Movie, Seq[UserRating])): RDD[UserRatingWithSimilarity] = {
    movies.sparkContext.parallelize(s._2).map(ur => (ur.user, ur.rating)).join(closestNeighbours).map(urs => UserRatingWithSimilarity(urs._1, urs._2._1, urs._2._2))
  }
}

object MoviesRecommender {

  def standardPrediction(user: UserAverageRating, neighbours: Seq[NeighbourInfo], neighboursRatings: Seq[UserRating]): Option[Double] = {
    val neighboursSimiliarityMap = neighbours.groupBy(_.neighbourName).mapValues(_.map(_.similarity))
    val ratingWithSimiliarity = neighboursRatings.map(r => (r.user, r.rating, neighboursSimiliarityMap.getOrElse(r.user, Seq.empty).head))
    val counter = ratingWithSimiliarity.map(userRating => userRating._2.getOrElse(0.0) * userRating._3).sum
    val delimeter = ratingWithSimiliarity.map(ur => if (ur._2.isEmpty) 0 else ur._3).sum

    if (delimeter == 0) Option.empty else Option(counter / delimeter)
  }

  //  def averageNormalizedPrediction(user: UserAverageRating, neighbours: Seq[NeighbourInfo], neighboursRatings: Seq[UserRating]): Option[Double] = {
  //    val neighboursSimiliarityMap = neighbours.groupBy(_._1).mapValues(_.map(_._2))
  //    val ratingWithSimiliarity = neighboursRatings.map(r => (r.user, r.rating, neighboursSimiliarityMap.getOrElse(r.user, Seq.empty).head))
  //    val counter = ratingWithSimiliarity.map(userRating => userRating._2.getOrElse(0.0) * userRating._3).sum
  //    val delimeter = ratingWithSimiliarity.map(ur => if (ur._2.isEmpty) 0 else ur._3).sum
  //
  //    if (delimeter == 0) Option.empty else Option(counter / delimeter)
  //  }

}


case class UserRatingWithSimilarity(user: String, rating: Option[Double], similarity: Double)

