package benchmark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.log4j.Level


/**
  * WordCount configuration with defaults.
  */

class GenerateTextBenchmarkData(
    sc: SparkContext,
    name: String,
    conf: TextGenConfig = new TextGenConfig(),
    numRuns: Int = 1,
    trimBy: Int = 0
    ) extends Benchmark(sc: SparkContext, name: String, conf: BenchmarkConfig, numRuns: Int, trimBy: Int) {
  /**
    * To be overriden by subclasses
    */
  override def run: Unit = {
    // Generate the text file
    time("Time to generate file") {
      val fileName = DataGeneration.generateText(conf.totalWords.getVal, conf.wordsPerLine.getVal, true)
      saveProperty("File size", fileSize(fileName))
    }
  }
}

class GenerateGraphBenchmarkData(
   sc: SparkContext,
   name: String,
   conf: GraphGenConfig = new GraphGenConfig(),
   numRuns: Int = 1,
   trimBy: Int = 0
   ) extends Benchmark(sc: SparkContext, name: String, conf: BenchmarkConfig, numRuns: Int, trimBy: Int) {
  /**
    * To be overriden by subclasses
    */
  override def run: Unit = {
    // Generate the text file
    time("Time to generate file") {
      val fileName = DataGeneration.generateGraph(conf.iterations.getVal, conf.v00, conf.v01, conf.v10, conf.v11, true)
      saveProperty("File size", fileSize(fileName))
    }
  }
}