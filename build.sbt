val scala3Version = "2.13.13"

lazy val root = project
  .in(file("."))
  .settings(
    name := "LLM-hw1",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := scala3Version,
    //    assembly / mainClass := Some("main.scala.MyMapReduceJob"),

    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "1.0.0",
      "org.apache.hadoop" % "hadoop-common" % "3.4.0",
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.4.0",
      "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "3.4.0",
      "org.apache.mrunit" % "mrunit" % "1.1.0",
      "com.knuddels" % "jtokkit" % "0.6.1",
      "com.typesafe" % "config" % "1.4.3",
      "org.deeplearning4j" % "deeplearning4j-core" % "1.0.0-M2.1", // Latest version as of now
      "org.deeplearning4j" % "deeplearning4j-nlp" % "1.0.0-M2.1", // NLP support
      "org.nd4j" % "nd4j-native-platform" % "1.0.0-M2.1",
      "org.nd4j" % "nd4j-native" % "1.0.0-M2.1" classifier "macosx-arm64",
      "org.slf4j" % "slf4j-simple" % "2.0.13", // Optional logging
      //"org.scalatest" %% "scalatest" % "3.2.14"% "test",
      "junit" % "junit" % "4.13.2" % "test",
      //"org.mockito" %% "mockito-scala" % "1.17.7"% "test"
      "org.scalatest" %% "scalatest" % "3.2.15" % "test",
      "org.mockito" %% "mockito-scala" % "1.17.14" % "test",
      "org.apache.mrunit" % "mrunit" % "1.1.0" % "test" classifier "hadoop2",
      "org.mockito" % "mockito-core" % "3.12.4" % "test"
),

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) =>
        xs match {
          case "MANIFEST.MF" :: Nil => MergeStrategy.discard
          case "services" :: _      => MergeStrategy.concat
          case _                    => MergeStrategy.discard
        }
      case "reference.conf"                => MergeStrategy.concat
      case x if x.endsWith(".proto")       => MergeStrategy.discard  // Changed to discard
      case x if x.contains("hadoop")       => MergeStrategy.first
      case x if x.contains("google")       => MergeStrategy.first
      case x if x.contains("protobuf")     => MergeStrategy.first
      case _ => MergeStrategy.first
    }
  )
