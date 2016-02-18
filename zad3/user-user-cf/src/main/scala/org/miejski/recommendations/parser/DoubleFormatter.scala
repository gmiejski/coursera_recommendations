package org.miejski.recommendations.parser

trait DoubleFormatter {
  def format(value: Double): Double = {
    "%.3f".format(value).toDouble
  }
}
