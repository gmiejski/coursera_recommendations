package org.miejski.recommendations.evaluation

import org.apache.spark.rdd.RDD
import org.miejski.recommendations.evaluation.model.User
import org.miejski.recommendations.model.UserRating
import org.miejski.recommendations.neighbours.Neighbours
import org.miejski.recommendations.recommendation.MoviesRecommender

class RecommenderEvaluator {

  def evaluateRecommender(usersRatings: RDD[User]) = {

    //    val sortedRatingsByTime = usersRatings.map(user => User(user.id, user.ratings.sortBy(_.rating)))

    val crossValidationSplit = new CrossValidationPartitioner().allCombinations(usersRatings)

    crossValidationSplit.map(crossValidation => {
      val furtherTestDataSplit = crossValidation.testData.map(user => {
        val c = user.ratings.length
        val trainingCount: Int = math.max(c - 10, 2 / 3 * c)
        val trainingPart = user.ratings.take(trainingCount)
        val testPart = user.ratings.drop(trainingCount)
        (User(user.id, trainingPart), User(user.id, testPart))
      })

      val realTestUsers = furtherTestDataSplit.map(_._2)
      val realTrainingUsers = furtherTestDataSplit.map(_._1).union(crossValidation.trainingData.reduce(_ union _))

      val moviesRatings = realTrainingUsers.flatMap(s => s.ratings.map(rating => (rating.movie, UserRating(s.id, rating.rating))))
        .groupByKey().map(s => (s._1, s._2.toSeq))
      val neighbours: Neighbours = null
      new MoviesRecommender(neighbours, moviesRatings, MoviesRecommender.standardPrediction)
    })
  }
}