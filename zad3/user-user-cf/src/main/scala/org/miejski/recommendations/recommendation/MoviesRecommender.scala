package org.miejski.recommendations.recommendation

import org.apache.spark.rdd.RDD
import org.miejski.recommendations.model.{Movie, UserRating}
import org.miejski.recommendations.neighbours.Neighbours
import org.miejski.recommendations.parser.DoubleFormatter

class MoviesRecommender(neighbours: Neighbours, moviesRatings: RDD[(Movie, Seq[UserRating])]) extends Serializable
  with DoubleFormatter {

  def forUser(user: String, top: Int = 0): Seq[(Movie, Double)] = {
    val movies = moviesRatings.map(_._1)
    val closestNeighbours: Seq[(String, Double)] = neighbours.findFor(user)
    val closestNeighboursIds = closestNeighbours.map(_._1)

    val neighboursRatings = moviesRatings.map(mRating => (mRating._1, mRating._2.filter(userRating => closestNeighboursIds.contains(userRating.user))))
    val predictedRatings = neighboursRatings
      .map(nr => (nr._1, MoviesRecommender.predictRating(closestNeighbours, nr._2))).collect()

    val moviesSortedByPredictedRating = predictedRatings.filter(_._2.isDefined)
      .map(s => (s._1, s._2.get))
      .sortBy(s => s._2)
      .map(prediction => (prediction._1, format(prediction._2)))
      .reverse
    if (top <= 0) moviesSortedByPredictedRating else moviesSortedByPredictedRating.take(top)
  }

  def toNeighboursRatingWithSimiliarity(movies: RDD[Movie], closestNeighbours: RDD[(String, Double)], s: (Movie, Seq[UserRating])): RDD[UserRatingWithSimiliarity] = {
    movies.sparkContext.parallelize(s._2).map(ur => (ur.user, ur.rating)).join(closestNeighbours).map(urs => UserRatingWithSimiliarity(urs._1, urs._2._1, urs._2._2))
  }
}

object MoviesRecommender {
  def apply(neighbours: Neighbours, moviesRatings: RDD[(Movie, Seq[UserRating])]): MoviesRecommender = {
    new MoviesRecommender(neighbours, moviesRatings)
  }

  def predictRating(neighbours: Seq[(String, Double)], neighboursRatings: Seq[UserRating]): Option[Double] = {
    val neighboursSimiliarityMap = neighbours.groupBy(_._1).mapValues(_.map(_._2))
    val ratingWithSimiliarity = neighboursRatings.map(r => (r.user, r.rating, neighboursSimiliarityMap.getOrElse(r.user, Seq.empty).head))
    val counter = ratingWithSimiliarity.map(userRating => userRating._2.getOrElse(0.0) * userRating._3).sum
    val delimeter = ratingWithSimiliarity.map(ur => if (ur._2.isEmpty) 0 else ur._3).sum

    if (delimeter == 0) Option.empty else Option(counter / delimeter)
  }

}


case class UserRatingWithSimiliarity(user: String, rating: Option[Double], similiarity: Double)