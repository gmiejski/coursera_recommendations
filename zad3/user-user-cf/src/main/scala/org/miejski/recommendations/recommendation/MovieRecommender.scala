package org.miejski.recommendations.recommendation

import org.miejski.recommendations.evaluation.model.{MovieRating, User}
import org.miejski.recommendations.model.Movie

trait MovieRecommender {
  def findRatings(user: User): List[MovieRating]

  def forUser(user: String, top: Int = 0): Seq[(Movie, Double)]
}
