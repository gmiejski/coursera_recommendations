package org.miejski.recommendations.model

case class UserRating(user: String, rating: Option[Double])

case class Movie(id: String, title: String)

object Movie {
  def apply(movieWithId: String): Movie = {
    val delimeterIndex = movieWithId.indexOf(":")
    Movie(movieWithId.substring(0, delimeterIndex), movieWithId.substring(delimeterIndex + 1).trim)
  }
}

case class MoviePredictedRating(movie: Movie, rating: Double)