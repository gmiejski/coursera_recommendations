package org.miejski.recommendations.evaluation

import org.miejski.recommendations.evaluation.model.User
import org.miejski.recommendations.helper.ShortCaseClasses
import org.scalatest.{FunSuite, Matchers}

class RecommenderEvaluatorTest extends FunSuite
  with Matchers
  with ShortCaseClasses {

  test("calculate root mean square errors between real and predicted ratings") {
    // given
    val userWithRealRatings = User("1", List(mr("1", 3.0), mr("3", 4.0), mr("5", 2.0)))
    val predictedRatings = List(mr("1", 3.0), mr("3", 2.0), mr("5", 3.0))

    //when
    val error = new RecommenderEvaluator().calculateRootMeanSquareError(userWithRealRatings, predictedRatings)

    // then
    error shouldBe 1.2909944487358056
  }
}
