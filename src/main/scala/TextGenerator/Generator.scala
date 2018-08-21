package TextGenerator

import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import java.io._
import scala.io.Source
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.log4j.Level


object Generator {

  def main(args: Array[String]): Unit = {

    val wordsPerLine = 50

    val conf = new SparkConf()
      .setAppName("TextGenerator")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.eventLog.enabled", "true")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rawWords = sc.textFile("/home/maddy/thesis-provenance/wordcount/words.txt").map(line => { val word::frequency::rank = line.split(",").toList; (word, frequency, rank) })
    val firstFilter = rawWords.map(x => (x._1, x._2, x._3(0)))
    val secondFilter = firstFilter.map( y => (y._1.substring(1), y._2, y._3.substring(1, y._3.indexOf(")"))))
    val thirdFilter = secondFilter.map(z =>  (z._1, z._2.toDouble, z._3.toInt))

    val rankedWords = thirdFilter.map(x => (x._1, x._3)).map(_.swap)
    val words = rankedWords.cache()

    for(i <- 1 to 10) {
      println(rankedWords.lookup(i).head)
    }
    words.saveAsTextFile("/tmp/text")

    val file = new File("/tmp/final.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    val bufferedSource = Source.fromFile("/tmp/ranks.txt")
    var i = 0
    for (line <- bufferedSource.getLines) {
      bw.append(rankedWords.lookup(line.toInt).head)
      if (i % wordsPerLine == 0)
        bw.write("\n")
    }
    bufferedSource.close
    bw.close()
  }
}
