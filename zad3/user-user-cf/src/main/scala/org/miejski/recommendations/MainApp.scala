package org.miejski.recommendations

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.miejski.recommendations.evaluation.model.{MovieRating, User}
import org.miejski.recommendations.evaluation.{CrossValidationPartitioner, RecommenderEvaluator}
import org.miejski.recommendations.neighbours.Neighbours
import org.miejski.recommendations.parser.RatingsReader
import org.miejski.recommendations.recommendation.CFMoviesRecommender

class MainApp {

}

object MainApp {

  val interestingUsers = List("3712", "3525", "3867", "89")

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setAppName("UserUserCollaborativeFiltering").setMaster("local[2]")
    val sc = SparkContext.getOrCreate(sparkConfig)

    val sqlContext = new SQLContext(sc)

    val ratingsDataframe = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("src/main/resources/coursera_recommendations_user-row.csv")

    val usersRatings = ratingsDataframe.rdd.map(RatingsReader.parseRatings)
    val neighbours = Neighbours(usersRatings)

    val neighboursFound = interestingUsers.map(user => (user, neighbours.findFor(user, 5)))

    val moviesRatings = RatingsReader.readRatings("src/main/resources/coursera_recommendations_movie-row.csv", sqlContext)

    val bestMoviesForUsers = interestingUsers.map(user => (user, new CFMoviesRecommender(neighbours, moviesRatings, CFMoviesRecommender.standardPrediction)
      .forUser(user, top = 3)))

    val bestNormalizedMoviesForUsers = interestingUsers.map(user => (user, new CFMoviesRecommender(neighbours, moviesRatings, CFMoviesRecommender.averageNormalizedPrediction)
      .forUser(user, top = 3)))

    val users = moviesRatings.flatMap(mR => mR._2.map(r => (r.user, MovieRating(mR._1, r.rating))))
      .filter(_._2.rating.nonEmpty)
      .groupByKey()
      .map(s => User(s._1, s._2.toList))

    val error = new RecommenderEvaluator().evaluateRecommender(users,
      (dataSplitter) => new CrossValidationPartitioner().allCombinations(dataSplitter),
      (n, mRatings) => new CFMoviesRecommender(neighbours, moviesRatings, CFMoviesRecommender.averageNormalizedPrediction))

    println(s"Final error : $error")
  }

}

