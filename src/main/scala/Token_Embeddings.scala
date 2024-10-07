import com.knuddels.jtokkit.Encodings
import com.knuddels.jtokkit.api.{Encoding, EncodingType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.deeplearning4j.nn.conf.layers.{EmbeddingLayer, OutputLayer}
import org.deeplearning4j.nn.conf.{MultiLayerConfiguration, NeuralNetConfiguration}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.nd4j.linalg.activations.Activation
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.lossfunctions.LossFunctions
import org.slf4j.LoggerFactory
import com.typesafe.config.ConfigFactory

import scala.jdk.CollectionConverters._

object Token_Embeddings {

  private val log = LoggerFactory.getLogger(getClass) // Logger instance
  private val config = ConfigFactory.load()
  private val environment = config.getString("environment")

  // This is the mapper class which generates token embeddings after getting the TokenIDs
  class EmbeddingMapper extends Mapper[LongWritable, Text, Text, Text] {
    private val outputKey = new Text() // Output key for the mapper
    private lazy val encoding: Encoding = Encodings.newDefaultEncodingRegistry().getEncoding(EncodingType.CL100K_BASE) // TokenIDs generation

    // Logging initialization of the mapper
    log.info("EmbeddingMapper initialized.")

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      // Split input value into sentences
      val sentences = value.toString.split("\n").toList

      // Tokenize sentences
      val tokenizedSentences: List[List[Integer]] = sentences.map(sentence =>
        encoding.encode(sentence).asScala.toList)

      // Preparing input features and output labels for training
      val flattenedTokens = tokenizedSentences.flatten.dropRight(1)
      val flattenedLabels = tokenizedSentences.flatten.drop(1)

      val inputFeatures: INDArray = Nd4j.create(flattenedTokens.map(_.toFloat).toArray, Array(flattenedTokens.size, 1))
      val outputLabels: INDArray = Nd4j.create(flattenedLabels.map(_.toFloat).toArray, Array(flattenedLabels.size, 1))

      // Creating and training the model
      val vocabSize = config.getInt("common.vocabSize")
      val embeddingDim = config.getInt("common.embeddingDim")
      val model = createModel(vocabSize, embeddingDim)
      trainModel(model, inputFeatures, outputLabels)

      // Getting learned embeddings and writing them to output
      val embeddings: INDArray = model.getLayer(0).getParam("W")
      writeEmbeddingsToOutput(flattenedTokens, embeddings, context)
    }

    // Creating the neural network model
    def createModel(vocabSize: Int, embeddingDim: Int): MultiLayerNetwork = {
      val config: MultiLayerConfiguration = new NeuralNetConfiguration.Builder()
        .list()
        .layer(new EmbeddingLayer.Builder()
          .nIn(vocabSize + 1)
          .nOut(embeddingDim)
          .activation(Activation.IDENTITY)
          .build())
        .layer(new OutputLayer.Builder(LossFunctions.LossFunction.SPARSE_MCXENT)
          .nIn(embeddingDim)
          .nOut(vocabSize + 1)
          .activation(Activation.SOFTMAX)
          .build())
        .build()

      val model = new MultiLayerNetwork(config) // Initializing the model
      model.init()
      log.info(s"Model initialized with vocabSize: $vocabSize and embeddingDim: $embeddingDim") // Log model initialization
      model
    }

    // Training the model with input features and output labels
    private def trainModel(model: MultiLayerNetwork, inputFeatures: INDArray, outputLabels: INDArray): Unit = {
      val numEpochs = config.getInt("common.numEpochs") // Number of training epochs
      log.info(s"Starting model training for $numEpochs epochs.") // Logging the start of training
      for (_ <- 0 until numEpochs) {
        model.fit(inputFeatures, outputLabels) // Training the model
      }
      log.info("Model training completed.") // Logging after training is complete
    }

    // Writing embeddings to the output
    private def writeEmbeddingsToOutput(tokens: List[Integer], embeddings: INDArray, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      tokens.foreach(tokenId => {
        val word = encoding.decode(java.util.List.of(tokenId)) // Decoding token ID to word
        outputKey.set(s"$word\t$tokenId") // Setting output key as "word tokenId"
        // Collect the embeddings for each token
        context.write(outputKey, new Text(embeddings.getRow(tokenId.longValue()).toStringFull))
      })
    }
  }

  // Reducer class calculates the average embeddings here
  class EmbeddingReducer extends Reducer[Text, Text, Text, Text] {
    override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
      val average = calculateAverage(values.asScala.toSeq) // Calculating average embeddings
      context.write(key, new Text(average.mkString("[", ", ", "]"))) // Collecting the average embedding
    }
  }

  // This is a method to parse the text representation of an array into Float array
  def parseArray(text: Text): Array[Float] = {
    text.toString
      .replace("[", "")
      .replace("]", "")
      .split(",")
      .map(_.trim.toFloat)
  }

  // This is a method to calculate average embedding from iterator
  def calculateAverage(values: Seq[Text]): Array[Float] = {
    val arrays = values.map(parseArray)
    val sumArray = arrays.reduce((a, b) => a.zip(b).map { case (x, y) => x + y })
    sumArray.map(_ / arrays.length)
  }

  // Main method is defined here
  def main(args: Array[String]): Unit = {
    try {
      log.info(s"Starting embedding job in $environment environment")
      log.info(s"Input path: ${config.getString(s"$environment.inputPath")}")
      log.info(s"Output path: ${config.getString(s"$environment.outputPath")}")

      val conf = new Configuration()
      val job = Job.getInstance(conf, config.getString(s"$environment.embedding.name"))
      job.setJarByClass(this.getClass)

      configureJob(job)

      log.info("Embedding job starting...")
      val success = job.waitForCompletion(true)
      if (success) {
        log.info("Embedding job completed successfully")
      } else {
        log.error("Embedding job failed to complete")
      }
    } catch {
      case e: Exception =>
        log.error("Error during embedding job execution", e)
        throw e
    }
  }

  // Method for Job Configuration
  def configureJob(job: Job): Unit = {
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    job.setMapperClass(classOf[EmbeddingMapper])
    job.setReducerClass(classOf[EmbeddingReducer])

    job.setNumReduceTasks(config.getInt(s"$environment.mapreduce.job.reduces"))

    FileInputFormat.addInputPath(job, new Path(config.getString(s"$environment.inputPath")))
    FileOutputFormat.setOutputPath(job, new Path(config.getString(s"$environment.outputPath")))

    // Set the split size from config
    FileInputFormat.setMaxInputSplitSize(job, config.getLong(s"$environment.mapreduce.input.fileinputformat.split.maxsize"))
  }
}