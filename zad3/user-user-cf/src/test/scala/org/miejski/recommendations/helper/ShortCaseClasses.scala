package org.miejski.recommendations.helper

import org.miejski.recommendations.evaluation.model.MovieRating
import org.miejski.recommendations.model.Movie

trait ShortCaseClasses {

  def mr(movieId: String, rating: Double): MovieRating = {
    MovieRating(m(movieId), Option.apply(rating))
  }

  def m(id: String): Movie = {
    Movie(id, id)
  }

}
