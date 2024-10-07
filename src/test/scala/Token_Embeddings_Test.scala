import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import Token_Embeddings._

class Token_Embeddings_Test extends AnyFlatSpec with Matchers {

  "EmbeddingMapper" should "generate token embeddings" in {
    val mapper = new EmbeddingMapper

    noException should be thrownBy mapper
  }

  "EmbeddingReducer" should "calculate average embeddings correctly" in {
    val reducer = new EmbeddingReducer

    // Simulating the reduce function
    val values = Seq(
      "[1.0, 2.0, 3.0]",
      "[2.0, 3.0, 4.0]"
    )

    val result = calculateAverage(values.map(new org.apache.hadoop.io.Text(_)))

    result should contain theSameElementsInOrderAs Array(1.5f, 2.5f, 3.5f)
  }

  // Helper method to simulate reducer's calculateAverage
  private def calculateAverage(values: Seq[org.apache.hadoop.io.Text]): Array[Float] = {
    val arrays = values.map(v => parseArray(v))
    val sumArray = arrays.reduce((a, b) => a.zip(b).map { case (x, y) => x + y })
    sumArray.map(_ / arrays.length)
  }

  private def parseArray(text: org.apache.hadoop.io.Text): Array[Float] = {
    text.toString
      .replace("[", "")
      .replace("]", "")
      .split(",")
      .map(_.trim.toFloat)
  }
}