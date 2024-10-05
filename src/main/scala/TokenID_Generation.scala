import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

import java.io.IOException
import scala.jdk.CollectionConverters._
import com.knuddels.jtokkit.Encodings
import com.knuddels.jtokkit.api.EncodingType

object TokenID_Generation {

  class TokenizerMapper extends Mapper[LongWritable, Text, Text, Text] {
    private val encoding = Encodings.newDefaultEncodingRegistry().getEncoding(EncodingType.CL100K_BASE)
    private val wordText = new Text()
    private val tokenText = new Text()

    @throws[IOException]
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      val words = value.toString.split("\\s+")
      words.foreach { word =>
        if (word.nonEmpty) {
          val tokens = encoding.encode(word)
          val tokenString = "[" + (0 until tokens.size()).map(tokens.get).mkString(",") + "]"
          wordText.set(word)
          tokenText.set(tokenString)
          context.write(wordText, tokenText)
        }
      }
    }
  }

  class TokenSumReducer extends Reducer[Text, Text, Text, Text] {
    override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
      val tokenSeq = values.asScala.toSeq

      val tokenFrequencyMap = tokenSeq
        .map(_.toString)
        .groupBy(identity)
        .map { case (token, occurrences) => (token, occurrences.size) }

      tokenFrequencyMap.foreach { case (token, frequency) =>
        context.write(key, new Text(s"$token $frequency"))
      }
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: TokenID_Generation <input path> <output path>")
      System.exit(-1)
    }

    val conf = new Configuration()
    val job = Job.getInstance(conf, "TokenFrequencyJob")
    job.setJarByClass(this.getClass)

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    job.setMapperClass(classOf[TokenizerMapper])
    job.setReducerClass(classOf[TokenSumReducer])

    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}