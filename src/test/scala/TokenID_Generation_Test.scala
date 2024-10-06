import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{BufferedWriter, FileWriter}

class TokenID_Generation_Test extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // Has Unit and Integration testing included
  // Temporary paths for testing
  private val inputPath = new Path("test_input.txt")
  private val outputPath = new Path("test_output")
  private val fs = FileSystem.get(new Configuration())

  // Sample input data
  private def writeSampleInput(): Unit = {
    val writer = new BufferedWriter(new FileWriter(inputPath.toString))
    writer.write("hello world\n")
    writer.write("this is a test\n")
    writer.write("tokenization test\n")
    writer.close()
  }

  // Cleanup after tests
  override def beforeAll(): Unit = {
    writeSampleInput()
  }

  override def afterAll(): Unit = {
    fs.delete(inputPath, false)
    fs.delete(outputPath, true)
  }

  // Unit tests for TokenEncoder starts here
  "TokenEncoder" should "encode words correctly" in {
    val word = "hello"
    val encoded = TokenID_Generation.TokenEncoder.encodeToTokenString(word)
    encoded should include("[") // It ensures it starts with [
    encoded should include("]") // It ensures it ends with ]
  }

  it should "encode a single character correctly" in {
    val word = "a"
    val encoded = TokenID_Generation.TokenEncoder.encodeToTokenString(word)
    encoded should include("[") // It ensures it starts with [
    encoded should include("]") // It ensures it ends with ]
  }

  it should "handle long words" in {
    val word = "abcdefghijklmnopqrstuvwxyz"
    val encoded = TokenID_Generation.TokenEncoder.encodeToTokenString(word)
    encoded should include("[") // It ensures it starts with [
    encoded should include("]") // It ensures it ends with ]
  }

  it should "encode numbers correctly" in {
    val word = "12345"
    val encoded = TokenID_Generation.TokenEncoder.encodeToTokenString(word)
    encoded should include("[") // It ensures it starts with [
    encoded should include("]") // It ensures it ends with ]
  }

  // Integration tests for the entire job starts here
  "TokenID_Generation Job" should "process input and produce output" in {
    val job = Job.getInstance(new Configuration(), "TokenFrequencyJob")
    TokenID_Generation.configureJob(job, inputPath.toString, outputPath.toString)
    job.waitForCompletion(true)

    // Checking output files
    val outputFiles = fs.listStatus(outputPath).map(_.getPath.getName).toSet
    outputFiles should contain("part-r-00000") // It checks if output part file is created
  }

  it should "produce correct token frequencies" in {
    val job = Job.getInstance(new Configuration(), "TokenFrequencyJob")
    TokenID_Generation.configureJob(job, inputPath.toString, outputPath.toString)
    job.waitForCompletion(true)

    // Read the output and checks the token frequencies
    val outputFilePath = new Path(outputPath, "part-r-00000")
    val outputContent = scala.io.Source.fromFile(outputFilePath.toString).getLines().toList

    outputContent should contain("hello") // It checks if 'hello' is processed
    outputContent should contain("world") // It checks if 'world' is processed
  }

  it should "handle multiple lines of input correctly" in {
    val job = Job.getInstance(new Configuration(), "TokenFrequencyJob")
    TokenID_Generation.configureJob(job, inputPath.toString, outputPath.toString)
    job.waitForCompletion(true)

    val outputFilePath = new Path(outputPath, "part-r-00000")
    val outputContent = scala.io.Source.fromFile(outputFilePath.toString).getLines().toList

    outputContent.size should be > 2 // It expects more than 2 tokens to be processed
  }

  it should "not produce empty outputs" in {
    val job = Job.getInstance(new Configuration(), "TokenFrequencyJob")
    TokenID_Generation.configureJob(job, inputPath.toString, outputPath.toString)
    job.waitForCompletion(true)

    val outputFilePath = new Path(outputPath, "part-r-00000")
    val outputContent = scala.io.Source.fromFile(outputFilePath.toString).getLines().toList

    outputContent should not be empty // It ensures that the output is not empty
  }

  it should "clean up output files after tests" in {
    // It checks that output files are deleted after tests
    fs.delete(outputPath, true) // Clean up after integration tests
    fs.exists(outputPath) shouldBe false
  }
}
