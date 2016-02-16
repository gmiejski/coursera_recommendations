package org.miejski.recommendations.neighbours

import org.apache.spark.rdd.RDD
import org.miejski.recommendations.correlation.PearsonCorrelation

class Neighbours(topNeighbours: RDD[(String, Seq[(String, Double)])]) {

  def findFor(user: String) = {
    topNeighbours.filter(n => n._1.equals(user)).flatMap(n => n._2)
  }

  def printNeighbours(user: String, top: Int = 5) = {
    println(s"Neighbours for user: $user")
    findFor(user).collect().take(top).foreach(println)
  }
}

object Neighbours {

  def apply(userRatings: RDD[(String, scala.Seq[Option[Double]])], uM: RDD[(String, String)]): Neighbours = {

    val uniqueMappings = uM.map(r => (r, None)) // for join with correlations
    assert(uniqueMappings.count() == 325)
    val joinedUsers = userRatings.cartesian(userRatings).cache()

    val lengths = joinedUsers.map(s => (s._1._2.length, s._2._2.length)).collect()
    val correlations = joinedUsers.map(ratings => ((ratings._1._1, ratings._2._1), PearsonCorrelation.compute(ratings._1._2, ratings._2._2)))
    val uniqueUsersCorrelations = uniqueMappings.join(correlations).map(s => (s._1, s._2._2))

    val topNeighbours = uniqueUsersCorrelations.filter(c => !c._1._1.equals(c._1._2))
      .flatMap(corr => Seq(corr, (corr._1.swap, corr._2)))
      .map(corr => (corr._1._1, (corr._1._2, corr._2)))
      .groupByKey()
      .map(s => (s._1, s._2.toSeq.sortBy(singleCorr => singleCorr._2).reverse))

    new Neighbours(topNeighbours)
  }


}
