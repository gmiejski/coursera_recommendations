package org.miejski.recommendations.parser

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.miejski.recommendations.model.{Movie, UserRating}

class RatingsReader {

}

object RatingsReader {
  def readRatings(file: String, sqlContext: SQLContext): RDD[(Movie, Seq[UserRating])] = {
    val moviesRatingsDataframe: DataFrame = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(file)

    val users = moviesRatingsDataframe.columns.drop(1).toSeq

    val moviesRatings = moviesRatingsDataframe
      .map(parseRatings)

    val ratings = moviesRatings.map(s => (Movie(s._1), s._2.zip(users).map(s => UserRating(s._2, s._1))))

    ratings
  }

  def parseRatings(row: Row): (String, Seq[Option[Double]]) = {
    val values = row.toSeq
    val userId: String = values.head.toString
    val b = (userId, values.drop(1).map(toOptionalRating))
    b
  }

  def toOptionalRating: (Any) => Option[Double] = {
    case x: String => if (x.length > 0) try {
      Some(x.replaceAll("\"", "").replaceAll(",", ".").toDouble)
    } catch {
      case _: Throwable => None
    } else None
    case x: Integer => Option(x.toDouble)
    case _ => None
  }
}
