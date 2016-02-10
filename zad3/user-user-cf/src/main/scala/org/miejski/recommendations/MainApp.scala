package org.miejski.recommendations

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.miejski.recommendations.correlation.PearsonCorrelation

class MainApp {

}

object MainApp {

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setAppName("UserUserCollaborativeFiltering").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(sparkConfig)

    val sqlContext = new SQLContext(sc)


    val dataframe = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("src/main/resources/coursera_recommendations_user-row.csv")

    //    dataframe.map(r => r.toSeq.asInstanceOf[Seq[Double]]).collect()
    val ratings = dataframe.map(row => row.toSeq.map(s => s match {
      case x: String => if (x.length > 0) x.replaceAll("\"", "").replaceAll(",", ".").toDouble else null
      case _ => null
    })).collect()

    val a = getUserRatings("1648", dataframe)
    val b = getUserRatings("3525", dataframe)

    PearsonCorrelation.compute(a, b)

    val data = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val rdd = sc.parallelize(data)

    rdd.map(s => s + 1).collect().foreach(println)
  }

  def getUserRatings(userId: String, dataframe: DataFrame): Seq[Double] = {
    val a = dataframe
      .where(s"userId = $userId")
      .map(row => row.toSeq.map(s => s match {
        case x: String => if (x.length > 0) x.replaceAll("\"", "").replaceAll(",", ".").toDouble else null
        case _ => null
      })).first()

    a.asInstanceOf[Seq[Double]]
  }
}


