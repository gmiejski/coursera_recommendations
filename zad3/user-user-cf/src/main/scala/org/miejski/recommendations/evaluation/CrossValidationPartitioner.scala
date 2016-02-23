package org.miejski.recommendations.evaluation

import org.apache.spark.rdd.RDD
import org.miejski.recommendations.evaluation.model.User

class CrossValidationPartitioner {

  def split(usersRatings: RDD[User], k: Int = 5): List[RDD[User]] = {
    val weights = List.fill(k)(1.0 / k)
    usersRatings.randomSplit(weights.toArray, 1).toList
  }
}
