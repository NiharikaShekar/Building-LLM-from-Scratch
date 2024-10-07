import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}

import java.io.IOException
import scala.jdk.CollectionConverters._
import com.knuddels.jtokkit.Encodings
import com.knuddels.jtokkit.api.EncodingType
import org.slf4j.LoggerFactory

object TokenID_Generation {

  // Initializing SLF4J Logger
  private val logger = LoggerFactory.getLogger(TokenID_Generation.getClass)

  private val config = ConfigFactory.load()
  private val environment = config.getString("environment")

  // Utility object for encoding using CL100K_BASE
  object TokenEncoder {
    private val encoding = Encodings.newDefaultEncodingRegistry().getEncoding(EncodingType.CL100K_BASE)

    def encodeToTokenString(word: String): String = {
      val tokens = encoding.encode(word)
      "[" + (0 until tokens.size()).map(tokens.get).mkString(",") + "]"
    }
  }

  // Mapper class for generating tokens for each word in a line
  class TokenizerMapper extends Mapper[LongWritable, Text, Text, Text] {
    private val wordText = new Text()
    private val tokenText = new Text()

    @throws[IOException]
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      val words = value.toString.split("\\s+")
      words.filter(_.nonEmpty).foreach { word =>
        val tokenString = TokenEncoder.encodeToTokenString(word)
        wordText.set(word)
        tokenText.set(tokenString)
        context.write(wordText, tokenText)
      }
    }
  }

  // This is a reducer class for aggregating token frequencies
  class TokenSumReducer extends Reducer[Text, Text, Text, Text] {

    // Method to create a frequency map of tokens
    private def createTokenFrequencyMap(values: Seq[String]): Map[String, Int] = {
      values.groupBy(identity).view.mapValues(_.size).toMap
    }

    override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
      val tokenSeq = values.asScala.toSeq.map(_.toString)
      val tokenFrequencyMap = createTokenFrequencyMap(tokenSeq)

      // Here each token and its frequency is written
      tokenFrequencyMap.foreach { case (token, frequency) =>
        context.write(key, new Text(s"$token $frequency"))
      }
    }
  }

  // Main method is defined here
  def main(args: Array[String]): Unit = {
    // Logging job configuration details
    logger.info(s"Configuring job with environment: $environment")

    // Status of Job
    try {
      val conf = new Configuration()
      val job = Job.getInstance(conf, config.getString(s"$environment.job.name"))
      job.setJarByClass(this.getClass)

      configureJob(job)

      logger.info("Job starting...")
      val success = job.waitForCompletion(true)
      if (success) {
        logger.info("Job completed successfully")
      } else {
        logger.error("Job failed to complete")
      }
    } catch {
      case e: Exception =>
        logger.error("Error during job execution", e)
    }
  }

  // Method for Job Configuration
  def configureJob(job: Job): Unit = {
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    job.setMapperClass(classOf[TokenizerMapper])
    job.setReducerClass(classOf[TokenSumReducer])

    job.setNumReduceTasks(config.getInt(s"$environment.mapreduce.job.reduces"))

    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])

    FileInputFormat.addInputPath(job, new Path(config.getString(s"$environment.inputPath")))
    FileOutputFormat.setOutputPath(job, new Path(config.getString(s"$environment.outputPath")))

    // Set the split size from config
    FileInputFormat.setMaxInputSplitSize(job, config.getLong(s"$environment.mapreduce.input.fileinputformat.split.maxsize"))
  }
}