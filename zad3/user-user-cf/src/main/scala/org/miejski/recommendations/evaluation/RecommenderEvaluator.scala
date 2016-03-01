package org.miejski.recommendations.evaluation

import org.apache.spark.rdd.RDD
import org.miejski.recommendations.evaluation.model.{MovieRating, User}
import org.miejski.recommendations.model.{Movie, UserRating}
import org.miejski.recommendations.neighbours.Neighbours
import org.miejski.recommendations.recommendation.MovieRecommender

class RecommenderEvaluator extends Serializable {

  def evaluateRecommender(usersRatings: RDD[User],
                          dataSplitter: (RDD[User]) => List[ValidationDataSplit],
                          recommenderCreator: (Neighbours, RDD[(Movie, Seq[UserRating])]) => MovieRecommender) = {
    //    val sortedRatingsByTime = usersRatings.map(user => User(user.id, user.ratings.sortBy(_.rating)))

    val validationDataSplit = dataSplitter(usersRatings)

    val foldsErrors = validationDataSplit.map(dataSplit => foldError(dataSplit, recommenderCreator))

    foldsErrors.sum / foldsErrors.length
  }

  def foldError(crossValidation: ValidationDataSplit,
                recommenderCreator: (Neighbours, RDD[(Movie, Seq[UserRating])]) => MovieRecommender): Double = {

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

    val recommender = recommenderCreator(neighbours, moviesRatings)

    val usersPredictionsErrors = realTestUsers.collect().map(user => calculateRootMeanSquareError(user, recommender.findRatings(user)))

    val singleFoldAvgError: Double = usersPredictionsErrors.sum / usersPredictionsErrors.length
    println(s"Single fold error: $singleFoldAvgError")
    singleFoldAvgError

  }

  def calculateRootMeanSquareError(user: User, predictedRatings: List[MovieRating]): Double = {
    assert(predictedRatings.length == user.ratings.length)
    val ratingsGroupedByMovie: Map[Movie, List[MovieRating]] = (user.ratings ++ predictedRatings).groupBy(_.movie)
    val squaredRatingsDiff = ratingsGroupedByMovie
      .filter(_._2.count(_.rating.nonEmpty) == 2)
      .map(kv => kv._2.map(_.rating.get).reduce((a, b) => (a - b) * (a - b)))

    val singleUserError: Double = math.sqrt(squaredRatingsDiff.sum / predictedRatings.length)
    println(s"${user.id} error: $singleUserError")
    singleUserError
  }
}