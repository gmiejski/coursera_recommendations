package org.miejski.recommendations.evaluation.model

import org.miejski.recommendations.model.Movie

case class MovieRating(movie: Movie, rating: Option[Double])

case class User(id: String, ratings: List[MovieRating])