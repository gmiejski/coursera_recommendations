package org.miejski.recommendations.evaluation

import org.apache.spark.rdd.RDD
import org.miejski.recommendations.evaluation.model.{MovieRating, User}
import org.miejski.recommendations.model.UserRating
import org.miejski.recommendations.neighbours.Neighbours
import org.miejski.recommendations.recommendation.MoviesRecommender

class RecommenderEvaluator extends Serializable {

  def evaluateRecommender(usersRatings: RDD[User]) = {
    //    val sortedRatingsByTime = usersRatings.map(user => User(user.id, user.ratings.sortBy(_.rating)))
    val crossValidationSplit = new CrossValidationPartitioner().allCombinations(usersRatings)

    val foldsErrors = crossValidationSplit.map(foldError)

    foldsErrors.sum / foldsErrors.length
  }

  def foldError(crossValidation: ValidationDataSplit): Double = {

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
    val neighbours: Neighbours = Neighbours.fromUsers(realTrainingUsers)
    val recommender = new MoviesRecommender(neighbours, moviesRatings, MoviesRecommender.standardPrediction)

    val usersPredictionsErrors = realTestUsers.map(user => calculateRootMeanSquareError(user, recommender.findRatings(user))).collect()

    usersPredictionsErrors.sum / usersPredictionsErrors.length

  }

  def calculateRootMeanSquareError(user: User, predictedRatings: List[MovieRating]): Double = {
    assert(predictedRatings.length == user.ratings.length)
    val squaredRatingsDiff = (user.ratings ++ predictedRatings).groupBy(_.movie)
      .map(kv => kv._2.map(_.rating.get).reduce((a, b) => (a - b) * (a - b)))

    math.sqrt(squaredRatingsDiff.sum / predictedRatings.length)
  }
}