package org.miejski.recommendations

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, Matchers}

trait SparkSuite extends FunSuite with Matchers {
  val sc = SparkContext.getOrCreate(new SparkConf().setAppName("SparkSuite").setMaster("local[4]"))

}
