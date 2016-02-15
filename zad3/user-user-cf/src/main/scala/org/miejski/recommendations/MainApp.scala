package org.miejski.recommendations

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.miejski.recommendations.correlation.PearsonCorrelation

class MainApp {

}

object MainApp {

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setAppName("UserUserCollaborativeFiltering").setMaster("local[2]")
    val sc = SparkContext.getOrCreate(sparkConfig)

    val sqlContext = new SQLContext(sc)

    val dataframe = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("src/main/resources/coursera_recommendations_user-row.csv")

    val ratings = dataframe.map(row => row.toSeq.map(s => s match {
      case x: String => if (x.length > 0) Some(x.replaceAll("\"", "").replaceAll(",", ".").toDouble) else None
      case _ => None
    }))

    val usersRatings = dataframe.rdd.map(parseRatings)
    val joinedUsers = usersRatings.cartesian(usersRatings).cache()
    val uniqueMappings = joinedUsers.map(r => (r._1._1, r._2._1))
      .map(toSortedUserJoin)
      .distinct()
      .map(r => (r, None)) // for join with correlations
    assert(uniqueMappings.count() == 325)

    val lengths = joinedUsers.map(s => (s._1._2.length, s._2._2.length)).collect()
    val correlations = joinedUsers.map(ratings => ((ratings._1._1, ratings._2._1), PearsonCorrelation.compute(ratings._1._2, ratings._2._2)))
    val uniqueUsersCorrelations = uniqueMappings.join(correlations).map(s => (s._1, s._2._2))


  }

  def toSortedUserJoin(userIds: (String, String)): (String, String) = {
    if (userIds._1 < userIds._2) userIds else userIds.swap
  }

  def parseRatings(row: Row): (String, Seq[Option[Double]]) = {
    val values = row.toSeq
    val userId: String = values.head.toString
    (userId, values.drop(1).map(toOptionalRating))
  }

  def getUserRatings(userId: String, dataframe: DataFrame): Seq[Option[Double]] = {
    dataframe
      .where(s"userId = $userId")
      .map(row => row.toSeq.map(toOptionalRating)).first()
  }

  def toOptionalRating: (Any) => Option[Double] = {
    case x: String => if (x.length > 0) try {
      Some(x.replaceAll("\"", "").replaceAll(",", ".").toDouble)
    } catch {
      case _: Throwable => None
    } else None
    case _ => None
  }
}


