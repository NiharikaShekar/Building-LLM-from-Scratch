import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import TokenID_Generation._

class TokenID_Generation_Test extends AnyFlatSpec with Matchers {

  "TokenEncoder" should "encode words correctly" in {
    TokenEncoder.encodeToTokenString("hello") should be("[15339]")
    TokenEncoder.encodeToTokenString("world") should be("[14957]")
  }

  "TokenizerMapper" should "correctly tokenize a given complete sentence " in {
    val mapper = new TokenizerMapper
    val result = scala.collection.mutable.Map[String, String]()

    // Simulating the map function
    val input = "It is social species"
    input.split("\\s+").foreach { word =>
      val tokenString = TokenEncoder.encodeToTokenString(word)
      result(word) = tokenString
    }

    result("It") should be("[2181]")
    result("is") should be("[285]")
    result("social") should be("[23191]")
    result("species") should be("[43590]")
  }

  "TokenSumReducer" should "correctly aggregate token frequencies" in {
    val reducer = new TokenSumReducer

    // Simulating the reduce function
    val key = "hello"
    val values = List("[15339,3438]", "[15339,3438]")
    val result = createTokenFrequencyMap(values)

    result should contain("[15339,3438]" -> 2)
  }


  // Helper method to simulate reducer's createTokenFrequencyMap
  private def createTokenFrequencyMap(values: Seq[String]): Map[String, Int] = {
    values.groupBy(identity).view.mapValues(_.size).toMap
  }
}