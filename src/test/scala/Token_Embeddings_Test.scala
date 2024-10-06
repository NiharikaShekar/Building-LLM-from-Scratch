import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred._
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class TokenEmbeddingsSpec extends AnyFlatSpec with Matchers {
  // Contains test cases for Unit and Integration Testing

  //Unit Testing starts here
  // Test cases for EmbeddingMapper
  "EmbeddingMapper" should "correctly tokenize sentences" in {
    val mapper = new Token_Embeddings.EmbeddingMapper()
    val sentences = new Text("Hello world.\nThis is a test.")
    val key = new LongWritable(1)
    val outputCollector = new CollectingOutputCollector()

    // Simulating the map function
    mapper.map(key, sentences, outputCollector, null)

    // Validating that outputCollector contains the expected tokens
    val output = outputCollector.getOutput
    output.size should be > 0
  }

  it should "train the model correctly" in {
    val mapper = new Token_Embeddings.EmbeddingMapper()

    val vocabSize = 100_000
    val embeddingDim = 20
    val model = mapper.createModel(vocabSize, embeddingDim)

    model shouldBe a[MultiLayerNetwork]
    model.getLayer(0) shouldEqual embeddingDim
  }



  it should "log model initialization" in {
    val mapper = new Token_Embeddings.EmbeddingMapper()
    val vocabSize = 100_000
    val embeddingDim = 20

  }


  // Test cases for EmbeddingReducer
  "EmbeddingReducer" should "calculate the average embeddings correctly" in {
    val reducer = new Token_Embeddings.EmbeddingReducer()
    val values = List(new Text("[1.0, 2.0, 3.0]"), new Text("[2.0, 4.0, 6.0]")).iterator.asJava

    val outputCollector = new CollectingOutputCollector()
    val key = new Text("test")

    reducer.reduce(key, values, outputCollector, null)

    // Verifying the average embedding calculation
    // The expected result should be [1.5, 3.0, 4.5]
    outputCollector.getOutput should contain (new Text("[1.5, 3.0, 4.5]"))
  }


  it should "calculate average for single value correctly" in {
    val reducer = new Token_Embeddings.EmbeddingReducer()
    val values = List(new Text("[3.0, 6.0, 9.0]")).iterator.asJava

    val outputCollector = new CollectingOutputCollector()
    val key = new Text("test")

    reducer.reduce(key, values, outputCollector, null)

    // Verifing that the output is as expected, e.g., [3.0, 6.0, 9.0]
    outputCollector.getOutput should contain (new Text("[3.0, 6.0, 9.0]"))
  }

  // Testing for embeddingMain
  "embeddingMain" should "run the embedding process" in {
    // Creating a temporary input/output path
    val inputPath = "hdfs://localhost:9000/temp/input"
    val outputPath = "hdfs://localhost:9000/temp/output"

    Token_Embeddings.embeddingMain(inputPath, outputPath)

  }

  it should "throw an exception when input path is missing" in {
    // Simulate missing input path
    val thrown = intercept[IllegalArgumentException] {
      Token_Embeddings.embeddingMain("", "outputPath") // Missing input path
    }
    thrown.getMessage should include("Usage: <input path> <output path>")
  }

  it should "throw an exception when output path is missing" in {
    // Simulate missing output path
    val thrown = intercept[IllegalArgumentException] {
      Token_Embeddings.embeddingMain("inputPath", "") // Missing output path
    }
    thrown.getMessage should include("Usage: <input path> <output path>")
  }
}

// CollectingOutputCollector is a simple class to collect output for testing
class CollectingOutputCollector extends OutputCollector[Text, Text] {
  private val output = scala.collection.mutable.ListBuffer.empty[(Text, Text)]

  override def collect(key: Text, value: Text): Unit = {
    output.append((key, value))
  }

  def getOutput: Seq[(Text, Text)] = output.toSeq
}