package org.miejski.recommendations

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.miejski.recommendations.neighbours.Neighbours
import org.miejski.recommendations.parser.RatingsReader
import org.miejski.recommendations.recommendation.MoviesRecommender

class MainApp {

}

object MainApp {

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

    val ratings = RatingsReader.readRatings("src/main/resources/coursera_recommendations_movie-row.csv", sqlContext)

    val recommendedMovies = MoviesRecommender(neighbours, ratings).forUser("3712")

    println()
  }

}

