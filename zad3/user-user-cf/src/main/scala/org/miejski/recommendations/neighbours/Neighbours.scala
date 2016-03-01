package org.miejski.recommendations.neighbours

import org.apache.spark.rdd.RDD
import org.miejski.recommendations.correlation.{NeighboursDetails, PearsonCorrelation}
import org.miejski.recommendations.evaluation.model.User

class Neighbours(topNeighbours: RDD[(String, Seq[NeighbourInfo])]) extends Serializable {

  def findFor(user: String, top: Int = 5) = {
    implicit val ord: Ordering[NeighbourInfo] = new Ordering[NeighbourInfo] {
      override def compare(x: NeighbourInfo, y: NeighbourInfo): Int = y.similarity.compare(x.similarity)
    }
    topNeighbours.filter(n => n._1.equals(user)).flatMap(_._2).takeOrdered(top)
  }

  def getUserAverageRating(user: String) = {

    topNeighbours.filter(s => !s._1.equals(user))
      .map(s => s._2.filter(_.neighbourName.equals(user)).head)
      .map(s => UserAverageRating(s.neighbourName, s.neighbourAverageRating))
      .collect().head
  }

  def printNeighbours(user: String, top: Int = 5) = {
    println(s"Neighbours for user: $user")
    findFor(user).take(top).foreach(println)
  }
}

object Neighbours {

  def fromUsers(userRatings: RDD[User]): Neighbours = {
    val joinedUsers = userRatings.cartesian(userRatings).cache()

    val uniqueUsersPairs: RDD[(String, String)] = joinedUsers.map(r => (r._1.id, r._2.id))
      .map(toSortedUserJoin)
      .distinct()

    val uniqueMappings = uniqueUsersPairs.map((_, None)) // for join with correlations

    val correlations = joinedUsers.map(ratings => ((ratings._1.id, ratings._2.id), PearsonCorrelation.compute(ratings._1.ratings, ratings._2.ratings)))
    val uniqueUsersCorrelations = uniqueMappings.join(correlations).map(s => (s._1, s._2._2))

    val topNeighbours: RDD[(String, Seq[NeighbourInfo])] = bidirectionalNeighboursMapping(uniqueUsersCorrelations)

    new Neighbours(topNeighbours)
  }

  def apply(userRatings: RDD[(String, scala.Seq[Option[Double]])]): Neighbours = {
    val joinedUsers = userRatings.cartesian(userRatings).cache()

    val uniqueUsersPairs: RDD[(String, String)] = joinedUsers.map(r => (r._1._1, r._2._1))
      .map(toSortedUserJoin)
      .distinct()

    val uniqueMappings = uniqueUsersPairs.map((_, None)) // for join with correlations

    val correlations = joinedUsers.map(ratings => ((ratings._1._1, ratings._2._1), PearsonCorrelation.compute(ratings._1._2, ratings._2._2)))
    val uniqueUsersCorrelations = uniqueMappings.join(correlations).map(s => (s._1, s._2._2))

    val topNeighbours: RDD[(String, Seq[NeighbourInfo])] = bidirectionalNeighboursMapping(uniqueUsersCorrelations)

    new Neighbours(topNeighbours)
  }

  def bidirectionalNeighboursMapping(uniqueUsersCorrelations: RDD[((String, String), NeighboursDetails)]): RDD[(String, Seq[NeighbourInfo])] = {
    val map: RDD[(String, NeighbourInfo)] = uniqueUsersCorrelations.filter(c => !c._1._1.equals(c._1._2))
      .flatMap(corr => Seq(
        (corr._1._1, NeighbourInfo(corr._1._2, corr._2.similarity, corr._2.user2SharedAverageRating, corr._2.user2AverageRating)),
        (corr._1._2, NeighbourInfo(corr._1._1, corr._2.similarity, corr._2.user1SharedAverageRating, corr._2.user1AverageRating))))
    val topNeighbours = map
      .groupByKey()
      .map(s => (s._1, s._2.toSeq.sortBy(singleCorr => singleCorr.similarity).reverse))
    topNeighbours
  }

  def toSortedUserJoin(userIds: (String, String)): (String, String) = {
    if (userIds._1 < userIds._2) userIds else userIds.swap
  }

}

case class NeighbourInfo(neighbourName: String,
                         similarity: Double,
                         neighbourSharedAverageRating: Double,
                         neighbourAverageRating: Double)

case class UserAverageRating(user: String, averageRating: Double)