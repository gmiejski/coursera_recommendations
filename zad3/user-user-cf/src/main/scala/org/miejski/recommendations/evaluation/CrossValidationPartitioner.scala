package org.miejski.recommendations.evaluation

import org.apache.spark.rdd.RDD
import org.miejski.recommendations.evaluation.model.User

class CrossValidationPartitioner {

  def allCombinations(usersRatings: RDD[User], k: Int = 5): List[ValidationDataSplit] = {
    val weights = List.fill(k)(1.0 / k)
    val partitions = usersRatings.randomSplit(weights.toArray, 1).toList

    val indexedPartitions = partitions.zipWithIndex
    indexedPartitions.map(iP => {
      ValidationDataSplit(iP._1, indexedPartitions.filter(p => !p._2.equals(iP._2)).map(_._1))
    })
  }
}

case class ValidationDataSplit(testData: RDD[User], trainingData: List[RDD[User]])