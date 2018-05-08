package benchmark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.log4j.Level




/**
 * Sort with Spark.
 */
class NativeSort(
    sc: SparkContext,
    name: String,
    conf: TextGenConfig = new TextGenConfig(),
    numRuns: Int = 1,
    trimBy: Int = 0) extends Benchmark(sc, name, conf, numRuns, trimBy) {

  override def run = {
    // Generate the text file
    val fileName = DataGeneration.generateText(conf.totalWords.getVal, conf.wordsPerLine.getVal, conf.runOnCluster)
    saveProperty("File size", fileSize(fileName))
    val nativeLines = sc.textFile(fileName).persist()
    val nativeSorted = nativeLines.map((_, 1))
      .sortByKey()
      .map { _._1 }

    time("Basic collect") {
      if(conf.runOnCluster) {
        nativeSorted.saveAsTextFile(s"${DataGeneration.s3BucketLink}tmp/bd-out-sort-native")
      }
      else {
        nativeSorted.saveAsTextFile("/tmp/bd-out")
      }
    }
  }
}
