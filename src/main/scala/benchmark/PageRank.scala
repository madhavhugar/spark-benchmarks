package benchmark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.log4j.Level


/**
 * PageRank configuration with defaults.
 */
class PageRankConfig extends GraphGenConfig {
  // How many iterations of the PageRank algorithm
  var prIterations: IntOrRange = 5

  override def toString() = {
    super.toString() + ", " +
      s"iterations = $prIterations"
  }

  override def getRangeElement: Option[(String, Int)] = {
    if (prIterations.isRange) {
      Some("prIterations", prIterations.getVal)
    } else {
      super.getRangeElement
    }
  }

  override def foreach(block: => Unit) = {
    if (prIterations.isInt) {
      super.foreach(block)
    } else if (!usingRange) {
      usingRange = true
      for (i <- prIterations.getRange) {
        prIterations.setVal(i)
        super.foreach(block)
      }
    } else {
      throw new IllegalArgumentException("Using more than one range in the Configuration")
    }
  }
}

/**
 * PageRank with Spark.
 */
class NativePageRank(
    sc: SparkContext,
    name: String,
    conf: PageRankConfig = new PageRankConfig(),
    numRuns: Int = 1,
    trimBy: Int = 0)
    extends Benchmark(sc, name, conf, numRuns, trimBy) {

  /**
   * Reads the textfile and returns the RDD.
   */
  def getLines(fileName: String): RDD[String] = {
    sc.textFile(fileName).filter { !_.startsWith("#") }
  }

  /**
   * Computes the PageRank.
   */
  def getRanks(lines: RDD[String]): RDD[(String, Double)] = {
    val graph = lines.map { s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }
    val nodes = graph.keys.distinct()
    val links = graph.groupByKey()
    var ranks = nodes.map(n => (n, 1.0))

    for (i <- 1 to conf.prIterations.getVal) {
      val contribs = links.join(ranks).values.flatMap {
        case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    ranks
  }

  override def run= {
    // Generate the graph file
    val fileName = DataGeneration.generateGraph(conf.iterations.getVal, conf.v00, conf.v01, conf.v10, conf.v11, conf.runOnCluster)

    val lines = getLines(fileName)
    saveProperty("Edges", lines.count())
    saveProperty("Nodes", lines.flatMap { _.split("\t").map { _.toInt } }.max())

    val ranks = getRanks(lines)
    time("Basic collect") {
        ranks.saveAsTextFile("/tmp/bd-out")
    }
  }
}
