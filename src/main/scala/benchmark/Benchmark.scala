package benchmark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.amazonaws.services.s3._
import com.amazonaws.AmazonServiceException
import scala.collection.immutable.{ SortedMap => Map }
import java.io._

/**
 * Information about one benchmark run
 */
class BenchmarkRun {
  var timeTaken: Map[String, Long] = Map()
  var properties: Map[String, Any] = Map()
}

/**
 * Base class for all benchmarks.
 */
abstract class Benchmark(
    sc: SparkContext,
    // A unique name for the benchmark, that is used to create a .dat file file with the results
    name: String,
    // A configuration
    conf: BenchmarkConfig = new BenchmarkConfig(),
    // How often to repeat each measurement
    numRuns: Int = 1,
    // How many entries to delete from start and end for the trimmed time averages
    trimBy: Int = 0) {
  
  val formatter = java.text.NumberFormat.getIntegerInstance

  // Where to write the graph text files.
  val path: String = if(conf.runOnCluster) { "/root/thesis/graphs/" } else { "/home/maddy/thesis-provenance/big-data-analysis/thesis/graphs/" }

  // Array of all runs
  var runsArray: Array[BenchmarkRun] = null
  
  // current index
  var currentRun = 0

  /**
   * To be overriden by subclasses
   */
  def run: Unit

  /**
   * Executes the benchmark n times and writes the results to a text file.
   * This text file can be used by PGFplots to create a plot.
   */
  def executeBenchmark() = {
    println(s"Starting benchmark $name")
    val file = new java.io.File(name)
    val bw = new BufferedWriter(new FileWriter(path + file + ".dat"))

    // Print configuration to the file
    bw.write(s"#Configuration: $conf\n")
    bw.flush()

    var headerWritten = false

    // This loop executes for each value in the range of the configuration,
    // or just once, if there is no range in the configuration.
    conf.foreach {
      runsArray = Array.fill(numRuns) { new BenchmarkRun() }
      currentRun = 0

      for (i <- 0 until numRuns) {
        run
        println(s"Finished run $currentRun.")

        // Write the header
        if (!headerWritten) {
          conf.getRangeElement.foreach { case (key, v) => bw.write(key + " ") }
          runsArray(currentRun).timeTaken.keys.foreach { key => bw.write(key + " ") }
          runsArray(currentRun).properties.keys.foreach { key => bw.write(key + " ") }
          bw.write("\n")
          bw.flush()
          headerWritten = true
        }

        currentRun += 1
      }
      // Group all measurements with the same key
      var times = Map[String, Seq[Long]]()
      runsArray.map(_.timeTaken).toSeq.foreach {
        map => map.foreach {
          entry => val seq = times.getOrElse(entry._1, Seq[Long]()); times = times + ((entry._1, entry._2 +: seq))
        }
      }
      // Sort by time, trim, calculate mean
      val avgs = times.mapValues { seq => val tr = seq.sorted.drop(trimBy).dropRight(trimBy); tr.sum / tr.size }
      
      // Write the results
      conf.getRangeElement.foreach { case (key, v) => bw.write(v + " ") }
      avgs.values.foreach { v => bw.write(v.toDouble / 1000000000 + " ") }
      runsArray(0).properties.values.foreach { v => bw.write(v + " ") }
      bw.write("\n")
      bw.flush()
    }

    bw.close()
  }

  /**
   * In Bytes.
   */
  def fileSize(name: String): Long = {
    if(conf.runOnCluster){
      val amazonS3Client = AmazonS3ClientBuilder.standard().build()
      val keyPath = new java.net.URI(name).getPath.substring(1)
      try {
        val fileMetaData = amazonS3Client.getObjectMetadata(DataGeneration.s3BucketName, keyPath)
        fileMetaData.getContentLength
      } catch {
        case awe: AmazonServiceException => awe.printStackTrace() ; 0
      }
    }
    else {
      val someFile = new java.io.File(name)
      someFile.length
    }
  }

  /**
   * Stores a property for the current run.
   */
  def saveProperty(key: String, value: Any) = {
    runsArray(currentRun).properties = runsArray(currentRun).properties + ((key.replaceAll("\\s", "_"), value))
  }

  /**
   * Stops the time for a given block of code and key name and saves it.
   */
  def time[R](key: String)(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    val t = t1 - t0
    runsArray(currentRun).timeTaken = runsArray(currentRun).timeTaken + ((key.replaceAll("\\s", "_"), t))
    result
  }
}

/**
 * Represents either a fixed nunber or a range. Can be used in a configuration.
 */
class IntOrRange(value: Either[Int, Range]) {
  def this(intVal: Int) = this(Left(intVal))
  def this(rangeVal: Range) = this(Right(rangeVal))
  def isInt = value.isLeft
  def isRange = value.isRight

  // Needs to be set by BenchmarkConfig.foreach, using setVal
  private var currentVal: Int = 0
  // Gets the current value
  def getVal: Int = if (isInt) value.left.get else currentVal
  // Sets the current value
  def setVal(i: Int) = {
    currentVal = i
  }

  // Gets the range if this is a range
  def getRange: Range = {
    value.right.get
  }

  override def toString() = {
    if (isInt) {
      value.left.get.toString()
    } else {
      s"$currentVal (in ${value.right.get})"
    }
  }
}

/**
 * Implicit conversions for IntOrRange
 */
object IntOrRange {
  implicit def intTo(i: Int) = new IntOrRange(i)
  implicit def rangeTo(r: Range) = new IntOrRange(r)
}

/**
 * Benchmark configuration with defaults.
 */
class BenchmarkConfig() {
  var iterationComputeProvenanceIndividually: Boolean = true
  var cacheLevel: StorageLevel = StorageLevel.MEMORY_ONLY
  var cacheComputation: Boolean = false
  var cacheIterations: Boolean = false
  var useStaticAnalysis: Boolean = false
  // Indicates if input data should be fetched from S3 or not, this parameter can be set true when run locally as well
  var runOnCluster: Boolean = false

  // Indicates if a range is used for some property. At most one range is allowed.
  protected var usingRange = false

  /**
   * Configures the current Benchmark run.
   */
  def configure = {
    // reduce the verbosity of Spark
    Logger.getRootLogger().setLevel(Level.ERROR)
    // Configure the ProvRDD class. See its documenation for details of the config values.

  }

  override def toString() = {
    s"iterationComputeProvenanceIndividually = $iterationComputeProvenanceIndividually, " +
      s"cacheLevel = $cacheLevel, " +
      s"cacheComputation = $cacheComputation, " +
      s"cacheIterations = $cacheIterations, " +
      s"useStaticAnalysis = $useStaticAnalysis"
  }

  /**
   * Executes a block for each value in a range, if the configuration contains a range.
   * Otherwise the block is executed just once.
   * 
   * Subclasses need to override it to account for the range behavior.
   */
  def foreach(block: => Unit) = {
    configure
    block
  }

  /**
   * This returns the range element (key, value), if there is a range.
   * 
   * Subclasses need to override this.
   */
  def getRangeElement: Option[(String, Int)] = {
    None
  }
}

/**
 * Configuration with parameters for text generation, too.
 */
class TextGenConfig extends BenchmarkConfig {
  // The number of words in total
  var totalWords: IntOrRange = 8000
  // The number of words per line = the spreading factor for a flatMap
  var wordsPerLine: IntOrRange = 100

  override def toString() = {
    super.toString() + ", " +
      s"totalWords = $totalWords, " +
      s"wordsPerLine = $wordsPerLine"
  }

  override def getRangeElement: Option[(String, Int)] = {
    if (totalWords.isRange) {
      Some("totalWords", totalWords.getVal)
    } else if (wordsPerLine.isRange) {
      Some("wordsPerLine", wordsPerLine.getVal)
    } else {
      super.getRangeElement
    }
  }

  override def foreach(block: => Unit) = {
    if (totalWords.isInt && wordsPerLine.isInt) {
      super.foreach(block)
    } else if (totalWords.isRange && wordsPerLine.isInt && !usingRange) {
      usingRange = true
      for (i <- totalWords.getRange) {
        totalWords.setVal(i)
        super.foreach(block)
      }
    } else if (totalWords.isInt && wordsPerLine.isRange && !usingRange) {
      usingRange = true
      for (i <- wordsPerLine.getRange) {
        wordsPerLine.setVal(i)
        super.foreach(block)
      }
    } else {
      throw new IllegalArgumentException("Using more than one range in the Configuration")
    }
  }
}

/**
 * Configuration with parameters for Kronecker graph generation, too.
 */
class GraphGenConfig extends BenchmarkConfig {
  var iterations: IntOrRange = 8
  var v00: Double = 0.65
  var v01: Double = 0.45
  var v10: Double = 0.4538
  var v11: Double = 0.4621

  override def toString() = {
    super.toString() + ", " +
      s"iterations = $iterations," +
      s"v00 = $v00" +
      s"v01 = $v01" +
      s"v10 = $v10" +
      s"v11 = $v11"
  }
  
  override def getRangeElement: Option[(String, Int)] = {
    if (iterations.isRange) {
      Some("iterations", iterations.getVal)
    } else {
      super.getRangeElement
    }
  }

  override def foreach(block: => Unit) = {
    if (iterations.isInt) {
      super.foreach(block)
    } else if (iterations.isRange && !usingRange) {
      usingRange = true
      for (i <- iterations.getRange) {
        iterations.setVal(i)
        super.foreach(block)
      }
    } else {
      throw new IllegalArgumentException("Using more than one range in the Configuration")
    }
  }
}
