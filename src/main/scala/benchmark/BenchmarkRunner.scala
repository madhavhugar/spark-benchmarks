package benchmark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scalax.file.Path
/**
 * Can be used to conduct different local benchmarks with time measurement.
 */
object BenchmarkRunner {

  /**
   * Implicit class to add a method to Ranges.
   */
  implicit class MyRange(val r: Range) {
    /**
     * "Strech" a range by a factor.
     */
    def *(i: Int): Range = {
      if (r.isInclusive) {
        r.start * i to r.end * i by r.step * i
      } else {
        r.start * i until r.end * i by r.step * i
      }
    }
  }

  def main(args: Array[String]) {

    //Choose to run benchmarks on cluster or locally
    val executeOnCluster = true

//    val path: Path = Path("/tmp/spark-events")
//    path.createDirectory(failIfExists=false)

    val conf = new SparkConf()
      .setAppName("Benchmark")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.eventLog.enabled", "true") // ensures that the spark app log is persisted
    if(!executeOnCluster) {
      conf.setMaster("local[2]")
    }

    val sc = new SparkContext(conf)

    // ------------
    // Application overhead, Section 4.1
    // ------------
  
/*       new NativeWordCount(sc, "nativeWordCount2",
         conf = new TextGenConfig {
           totalWords = (5 to 35 by 10) * 10000000
           wordsPerLine = 10000
           runOnCluster = executeOnCluster
         },
         numRuns = 8, trimBy = 2).executeBenchmark()*/

       new NativeSort(sc, "nativeSort",
         conf = new TextGenConfig {
           totalWords = (5 to 30 by 5) * 10000000
           wordsPerLine = 10000
           runOnCluster = executeOnCluster
         },
         numRuns = 5, trimBy = 1).executeBenchmark()

/*        new NativeGrep(sc, "nativeGrep",
          conf = new TextGenConfig {
            totalWords = (5 to 35 by 5) * 10000000
            wordsPerLine = 10000
            runOnCluster = executeOnCluster
          },
          numRuns = 8, trimBy = 2).executeBenchmark()*/
    /*
                   new NativeWordCount(sc, "nativeWordCount",
                     conf = new TextGenConfig {
                       totalWords = (20 to 50 by 5) * 10000000
                       wordsPerLine = 10000
                       runOnCluster = executeOnCluster
                       },
                       numRuns = 5, trimBy = 1).executeBenchmark()
                   new WordCount(sc, "wordCountCollect", queryBasic = true,
                      queryProvAll = false,
                      queryProvMaxWord = false,
                      conf = new WordCountConfig {
                       totalWords = (5 to 50 by 5) * 10000000
                       wordsPerLine = 10000
                       runOnCluster = executeOnCluster
                       },
                       numRuns = 5, trimBy = 1).executeBenchmark()

                   new NativeSort(sc, "nativeSort",
                     conf = new TextGenConfig {
                       totalWords = (5 to 15 by 1) * 10000000
                       wordsPerLine = 10000
                       runOnCluster = executeOnCluster
                     },
                     numRuns = 5, trimBy = 1).executeBenchmark()
                   new Sort(sc, "sortCollect", queryProv = false,
                     conf = new TextGenConfig {
                       totalWords = (5 to 15 by 1) * 10000000
                       wordsPerLine = 10000
                       runOnCluster = executeOnCluster
                     },
                     numRuns = 5, trimBy = 1).executeBenchmark()

                   new NativeGrep(sc, "nativeGrep",
                     conf = new TextGenConfig {
                       totalWords = (5 to 50 by 5) * 10000000
                       wordsPerLine = 10000
                       runOnCluster = executeOnCluster
                     },
                     numRuns = 5, trimBy = 1).executeBenchmark()

                   new Grep(sc, "grepCollect", queryProv = false,
                     conf = new TextGenConfig {
                       totalWords = (35 to 50 by 5) * 10000000
                       wordsPerLine = 10000
                       runOnCluster = executeOnCluster
                     },
                     numRuns = 5, trimBy = 1).executeBenchmark()

                   new NativePageRank(sc, "nativePageRank",
                     conf = new PageRankConfig {
                       prIterations = 1 to 10
                       iterations = 17
                       v00 = 0.65
                       v01 = 0.55
                       v10 = 0.55
                       v11 = 0.55
                       runOnCluster = executeOnCluster
                     },
                     numRuns = 5, trimBy = 1).executeBenchmark()
                   new PageRank(sc, "pageRankCollect", queryProv = false, queryExpl = false,
                     conf = new PageRankConfig {
                       prIterations = 1 to 10
                       iterations = 17
                       v00 = 0.65
                       v01 = 0.55
                       v10 = 0.55
                       v11 = 0.55
                       runOnCluster = executeOnCluster
                     },
                     numRuns = 5, trimBy = 1).executeBenchmark()

           */

    // ------------
    // Efficiency, Section 4.2.1
    // ------------
      
/*
        new WordCount(sc, "wordCount", queryBasic = false, queryProvSome = false, queryProvMaxWord = false, queryProvAll = false, queryProvSomeAscending = true,
          conf = new WordCountConfig {
            totalWords = (2 to 7) * 10000000
            wordsPerLine = 10000
            wordsToTrack = 1
            runOnCluster = executeOnCluster
          },
          numRuns = 5, trimBy = 1).executeBenchmark()

        new Sort(sc, "sort", queryBasic = false, queryProv = false, queryProvSome = true,
          conf = new WordCountConfig {
            totalWords = (1 to 10) * 10000000
            wordsPerLine = 10000
            wordsToTrack = 1
            runOnCluster = executeOnCluster
          },
          numRuns = 5, trimBy = 1).executeBenchmark()

        new Grep(sc, "grep", queryBasic = false, queryProv = false, queryProvSome = true,
          conf = new WordCountConfig {
            totalWords = (1 to 10) * 10000000
            wordsPerLine = 10000
            wordsToTrack = 1
            runOnCluster = executeOnCluster
          },
          numRuns = 5, trimBy = 1).executeBenchmark()
*/

    // ------------
    // ProvRDD.broadcastJoin Tests
    // ------------

//    new WordCount(sc, "wordCountMaxWord", queryBasic = false, queryProvSome = false, queryProvMaxWord = true, queryProvAll = false, queryProvSomeAscending = false,
//      conf = new WordCountConfig {
//        totalWords = (1) * 10000000
//        wordsPerLine = 10000
//        runOnCluster = executeOnCluster
//      },
//      numRuns = 3, trimBy = 1).executeBenchmark()

  //   wordCountBroadcastJoin test is bound to fail after one point of time. This test is to find that point. (this test can be ignored)

//    new WordCount(sc, "wordCountBroadcastJoin", queryBasic = false, queryProvSome = false, queryProvMaxWord = false, queryProvAll = false, queryProvSomeAscending = true,
//      conf = new WordCountConfig {
//        totalWords = (1) * 10000000
//        wordsPerLine = 10000
//        wordsToTrack = (5000 to 10000000 by 100)
//        runOnCluster = executeOnCluster
//      },
//      numRuns = 1, trimBy = 0).executeBenchmark()

    // ------------
    // Efficiency parameters, Section 4.2.2
    // ------------

    // Cache parameter

    //        new WordCount(sc, "wordCount-nocache", queryBasic = false, queryProvAll = false,
//          conf = new WordCountConfig {
//            totalWords = (1 to 10) * 10000000
//            wordsPerLine = 10000
//            wordsToTrack = 1
//            cacheComputation = false
//            runOnCluster = executeOnCluster
//          },
//          numRuns = 5, trimBy = 1).executeBenchmark()
//        new WordCount(sc, "wordCount-cache-mem", queryBasic = false, queryProvAll = false,
//          conf = new WordCountConfig {
//            totalWords = (1 to 10) * 10000000
//            wordsPerLine = 10000
//            wordsToTrack = 1
//            cacheComputation = true
//            cacheLevel = StorageLevel.MEMORY_ONLY
//            runOnCluster = executeOnCluster
//          },
//          numRuns = 5, trimBy = 1).executeBenchmark()
//        new WordCount(sc, "wordCount-cache-mem-ser", queryBasic = false, queryProvAll = false,
//         conf = new WordCountConfig {
//            totalWords = (1 to 10) * 10000000
//            wordsPerLine = 10000
//            wordsToTrack = 1
//            cacheComputation = true
//            cacheLevel = StorageLevel.MEMORY_ONLY_SER
//            runOnCluster = executeOnCluster
//          },
//          numRuns = 5, trimBy = 1).executeBenchmark()

     // Complete vs minimal provenance
//        new GraphPropagation(sc, "gptime-sparse",
//          conf = new GraphPropagationConfig {
//            graphFile = "src/main/resources/graph-sparse.txt"
//            propagationIterations = 1 to 6
//            nodeToTrack = 0
//            runOnCluster = executeOnCluster
//          },
//          numRuns = 5, trimBy = 1).executeBenchmark()
//        new GraphPropagation(sc, "gptime-increasing-size",
//          conf = new GraphPropagationConfig {
//            propagationIterations = 2
//            iterations = 13 to 18
//            v00 = 0.65
//            v01 = 0.55
//            v10 = 0.55
//            v11 = 0.55
//            nodeToTrack = 0
//            runOnCluster = executeOnCluster
//          },
//          numRuns = 5, trimBy = 1).executeBenchmark()

    // Iteration algorithm
    //    new UniqueChars(sc, "uc-iterations", queryBasic = false,
    //      conf = new CharGenConfig {
    //        iterations = 1 to 51 by 5
    //      },
    //      numRuns = 10, trimBy = 2).executeBenchmark()

    // ------------
    // Provenance size, Section 4.3
    // ------------
    //    new GraphPropagation(sc, "gpsize-sparse-2",
    //      conf = new GraphPropagationConfig {
    //        graphFile = "src/main/resources/graph-sparse.txt"
    //        propagationIterations = 2
    //        nodeToTrack = 0 to 127
    //      },
    //      numRuns = 1).executeBenchmark()
    //
    //    new GraphPropagation(sc, "gpsize-dense-2",
    //      conf = new GraphPropagationConfig {
    //        graphFile = "src/main/resources/graph-dense.txt"
    //        propagationIterations = 2
    //        nodeToTrack = 0 to 127
    //      },
    //      numRuns = 1).executeBenchmark()
    //
    //    new GraphPropagation(sc, "gpsize-sparse-3",
    //      conf = new GraphPropagationConfig {
    //        graphFile = "src/main/resources/graph-sparse.txt"
    //        propagationIterations = 3
    //        nodeToTrack = 0 to 127
    //      },
    //      numRuns = 1).executeBenchmark()
    //
    //    new GraphPropagation(sc, "gpsize-dense-3",
    //      conf = new GraphPropagationConfig {
    //        graphFile = "src/main/resources/graph-dense.txt"
    //        propagationIterations = 3
    //        nodeToTrack = 0 to 127
    //      },
    //      numRuns = 1).executeBenchmark()
    //
    //    new GraphPropagation(sc, "gpsize-sparse-4",
    //      conf = new GraphPropagationConfig {
    //        graphFile = "src/main/resources/graph-sparse.txt"
    //        propagationIterations = 4
    //        nodeToTrack = 0 to 127
    //      },
    //      numRuns = 1).executeBenchmark()
    //
    //    new GraphPropagation(sc, "gpsize-dense-4",
    //      conf = new GraphPropagationConfig {
    //        graphFile = "src/main/resources/graph-dense.txt"
    //        propagationIterations = 4
    //        nodeToTrack = 0 to 127
    //      },
    //      numRuns = 1).executeBenchmark()

    // ----------------
    // Other benchmarks
    // ----------------

    // Shows that explain gives reproducible results
    //    new NativeUniqueWords(sc, "NativeUniqueWords").executeBenchmark()
    //    new UniqueWords(sc, "UniqueWords").executeBenchmark()


    // Shows benefit of computing iterations individually
    //    new UniqueChars(sc, "UniqueChars", conf = new CharGenConfig {iterationComputeProvenanceIndividually = false})
    //      .executeBenchmark()

    //----------------------------
    //Upload Benchmarks Input to S3 (Should be run locally)
    //----------------------------

/*
    new GenerateTextBenchmarkData(sc, "ignore-me",
      conf = new TextGenConfig {
        totalWords = (1 to 15 by 1) * 10000000
        wordsPerLine = 10000
      },
      numRuns = 1, trimBy = 0).executeBenchmark()
    new GenerateTextBenchmarkData(sc, "ignore-me",
      conf = new TextGenConfig {
        totalWords = (5 to 50 by 5) * 10000000
        wordsPerLine = 10000
      },
      numRuns = 1, trimBy = 0).executeBenchmark()
    new GenerateGraphBenchmarkData(sc, "ignore-me",
       conf = new GraphGenConfig {
          iterations = 13 to 18
          v00 = 0.65
          v01 = 0.55
          v10 = 0.55
          v11 = 0.55
       },
       numRuns = 1, trimBy = 0).executeBenchmark()
*/

    println("Done with all benchmarks")
    sc.stop
  }
}
