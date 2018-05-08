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
class WordCountConfig extends TextGenConfig {
  // How many words to track for provenance/explain queries
  var wordsToTrack: IntOrRange = 1

  override def toString() = {
    super.toString() + ", " +
      s"wordsToTrack = $wordsToTrack"
  }

  override def getRangeElement: Option[(String, Int)] = {
    if (wordsToTrack.isRange) {
      Some("wordsToTrack", wordsToTrack.getVal)
    } else {
      super.getRangeElement
    }
  }

  override def foreach(block: => Unit) = {
    if (wordsToTrack.isInt) {
      super.foreach(block)
    } else if (!usingRange) {
      usingRange = true
      for (i <- wordsToTrack.getRange) {
        wordsToTrack.setVal(i)
        super.foreach(block)
      }
    } else {
      throw new IllegalArgumentException("Using more than one range in the Configuration")
    }
  }
}

/**
 * WordCount with Spark.
 */
class NativeWordCount(
    sc: SparkContext,
    name: String,
    conf: TextGenConfig = new TextGenConfig(),
    numRuns: Int = 1,
    trimBy: Int = 0)
    extends Benchmark(sc, name, conf, numRuns, trimBy) {

  /**
   * Benchmark for WordCount
   */
  override def run = {
    // Generate the text file
    val fileName = DataGeneration.generateText(conf.totalWords.getVal, conf.wordsPerLine.getVal, conf.runOnCluster)

    saveProperty("File size", fileSize(fileName))
    val nativeLines = sc.textFile(fileName)

    val nativeWordCounts = nativeLines
      .flatMap { x => x.split(" ") }
      .map((_, 1))
      .reduceByKey(_ + _)

    //var data: Array[_] = null
    time("Basic collect") {
      nativeWordCounts.saveAsTextFile(s"${DataGeneration.s3BucketLink}tmp/bd-out-wordcount-native")
    }
    //saveProperty("Collect count", data.size)
  }
}
