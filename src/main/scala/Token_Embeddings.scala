import com.knuddels.jtokkit.Encodings
import com.knuddels.jtokkit.api.{Encoding, EncodingRegistry, EncodingType}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import org.deeplearning4j.nn.conf.layers.{EmbeddingLayer, OutputLayer}
import org.deeplearning4j.nn.conf.{MultiLayerConfiguration, NeuralNetConfiguration}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.nd4j.linalg.activations.Activation
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.lossfunctions.LossFunctions
import org.nd4j.linalg.ops.transforms.Transforms

import java.io.IOException
import scala.io.Source
import scala.jdk.CollectionConverters._

object Token_Embeddings {

  class EmbeddingMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, Text] {
    private val outputKey = new Text()
    private lazy val encoding: Encoding = Encodings.newDefaultEncodingRegistry().getEncoding(EncodingType.CL100K_BASE)

    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, Text], reporter: Reporter): Unit = {
      val sentences = value.toString.split("\n").toList
      val tokenizedSentences: List[List[Integer]] = sentences.map(sentence => encoding.encode(sentence).asScala.toList)

      val vocabSize = 100_000
      val embeddingDim = 5

      val flattenedTokens = tokenizedSentences.flatMap(tokens => tokens.dropRight(1))
      val flattenedLabels = tokenizedSentences.flatMap(tokens => tokens.drop(1))

      val inputFeatures: INDArray = Nd4j.create(flattenedTokens.map(_.toFloat).toArray, Array(flattenedTokens.size, 1))
      val outputLabels: INDArray = Nd4j.create(flattenedLabels.map(_.toFloat).toArray, Array(flattenedLabels.size, 1))

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

      val model = new MultiLayerNetwork(config)
      model.init()

      val numEpochs = 2
      for (_ <- 0 until numEpochs) {
        model.fit(inputFeatures, outputLabels)
      }

      val embeddings: INDArray = model.getLayer(0).getParam("W")

      flattenedTokens.foreach(t => {
        val word = encoding.decode(java.util.List.of(t.intValue()))
        outputKey.set(word + "\t" + t)
        output.collect(outputKey, new Text(embeddings.getRow(t.longValue()).toStringFull))
      })
    }
  }

  class EmbeddingReducer extends MapReduceBase with Reducer[Text, Text, Text, Text] {
    override def reduce(key: Text, values: java.util.Iterator[Text], output: OutputCollector[Text, Text], reporter: Reporter): Unit = {
      val average = calculateAverage(values)
      output.collect(key, new Text(average.mkString("[", ", ", "]")))
    }
  }

  def parseArray(text: Text): Array[Float] = {
    text.toString
      .replace("[", "")
      .replace("]", "")
      .split(",")
      .map(_.trim.toFloat)
  }

  def calculateAverage(iterator: java.util.Iterator[Text]): Array[Float] = {
    var sumArray: Array[Float] = Array.empty
    var count = 0

    while (iterator.hasNext) {
      val currentArray = parseArray(iterator.next())

      if (sumArray.isEmpty) {
        sumArray = new Array[Float](currentArray.length)
      }

      for (i <- currentArray.indices) {
        sumArray(i) += currentArray(i)
      }

      count += 1
    }

    for (i <- sumArray.indices) {
      sumArray(i) /= count
    }

    sumArray
  }

  def embeddingMain(inputPath: String, outputPath: String): RunningJob = {
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("Embedder")
    conf.set("fs.defaultFS", "hdfs://localhost:9000")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[Text])
    conf.setMapperClass(classOf[EmbeddingMapper])
    conf.setReducerClass(classOf[EmbeddingReducer])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, Text]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)
  }

  def embeddingTest(): Unit = {
    val inputFilePath = "src/main/resources/input_WC.txt"
    val sentences = Source.fromFile(inputFilePath).getLines().toList

    val encodingRegistry: EncodingRegistry = Encodings.newDefaultEncodingRegistry()
    val encoding: Encoding = encodingRegistry.getEncoding(EncodingType.CL100K_BASE)

    val tokenizedSentences: List[List[Integer]] = sentences.map(sentence => encoding.encode(sentence).asScala.toList)

    val vocabSize = 100_000
    val embeddingDim = 5

    val flattenedTokens = tokenizedSentences.flatMap(tokens => tokens.dropRight(1))
    val flattenedLabels = tokenizedSentences.flatMap(tokens => tokens.drop(1))

    val inputFeatures: INDArray = Nd4j.create(flattenedTokens.map(_.toFloat).toArray, Array(flattenedTokens.size, 1))
    val outputLabels: INDArray = Nd4j.create(flattenedLabels.map(_.toFloat).toArray, Array(flattenedLabels.size, 1))

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

    val model = new MultiLayerNetwork(config)
    model.init()

    val numEpochs = 100
    for (_ <- 0 until numEpochs) {
      model.fit(inputFeatures, outputLabels)
    }

    val embeddings: INDArray = model.getLayer(0).getParam("W")

    println("Learned Embeddings:\n" + embeddings)

    val numWords = embeddings.rows()

    for (i <- 0 until numWords) {
      val embeddingVector = embeddings.getRow(i)
      val word = encoding.decode(java.util.List.of(i))
      println(s"Word: $word Token ID: $i, Embedding: ${embeddingVector}")
    }

    val helloTokenId = encoding.encode("Hello").asScala.head
    val worldTokenId = encoding.encode("world").asScala.head

    val sampleEmbedding1: INDArray = embeddings.getRow(helloTokenId.longValue())
    val sampleEmbedding2: INDArray = embeddings.getRow(worldTokenId.longValue())

    val similarity = Transforms.allCosineSimilarities(sampleEmbedding1, sampleEmbedding2)

    println(s"Cosine similarity between 'Hello' and 'world': $similarity")
  }

  def main(args: Array[String]): Unit = {
    if (args.length == 2) {
      val inputPath = args(0)
      val outputPath = args(1)
      embeddingMain(inputPath, outputPath)
    } else {
      embeddingTest()
    }
  }
}
