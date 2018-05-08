package benchmark

import java.io.File
import sys.process._
import java.nio.file.{Files, Paths}
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.AmazonServiceException


/**
 * Data Generation of text and graph, using the BDGS.
 */
object DataGeneration {
  
  val bdgsPath = "/home/maddy/thesis-provenance/big-data-analysis/BigDataGeneratorSuite/"
  val s3BucketName = "bigdatarepo"
  val s3BucketLink = s"s3n://${s3BucketName}/"
  val dataset = "lda_wiki1w"
  /**
   * Generates a text file with the given total words and words per line and
   * returns the path to the file.
   */
  def generateText(totalWords: Int, wordsPerLine: Int, runOnCluster: Boolean) : String = {

    if(runOnCluster) {
      getTextS3(totalWords, wordsPerLine)
    }
    else {
      val path = new java.io.File(bdgsPath + "Text_datagen")

      val outputPath = s"/tmp/textgen/${totalWords}-${wordsPerLine}"
      val outputFile = s"${outputPath}/${dataset}_1"

      // Only generate the file if it does not exist yet.
      if(!Files.exists(Paths.get(outputFile))) {
        val command = s"./gen_text_data.sh $dataset 1 ${totalWords / wordsPerLine} $wordsPerLine $outputPath"
        Process(command, path) !
      }

      outputFile
    }

  }

  def validateS3Path(keyPath: String) : Boolean = {
    var isValidKeyPath = true
    val amazonS3Client = AmazonS3ClientBuilder.standard().build()
    try {
      amazonS3Client.getObject(s3BucketName, keyPath)
    } catch {
      case awe: AmazonServiceException => isValidKeyPath = false
    }
    isValidKeyPath
  }

  def uploadFileToS3(f: => String, keyPath: String) : Boolean = {
    val amazonS3Client = AmazonS3ClientBuilder.standard().build()
    try {
      amazonS3Client.putObject(s3BucketName, keyPath, new File(f))
    } catch {
      case awe: AmazonServiceException => awe.printStackTrace()
    }
    true
  }

  def getTextS3(totalWords: Int, wordsPerLine: Int) : String = {
    val keyPath = s"textgen/$totalWords-$wordsPerLine/${dataset}_1"
    val s3FileUrl= s"${s3BucketLink}textgen/$totalWords-$wordsPerLine/${dataset}_1"
    if(!validateS3Path(keyPath)) {
      if(uploadFileToS3(generateText(totalWords, wordsPerLine, false), keyPath)) {
        println(s"Uploaded File: $keyPath to Bucket: $s3BucketName")
      }
    }
    println(s"Fetching file from S3: $s3FileUrl")
    s3FileUrl
  }

  /**
   * Generates a text file with the 5 * given lines and words per line and
   * returns the path to the file.
   * 
   * Currently only used by NBC, which is not part of the evaluation.
   */
  def generateClassifierText(lines: Int, wordsPerLine: Int) : String = {
    val path = new java.io.File(bdgsPath + "E-commerce")
    val command = s"./genData_naivebayes.sh $lines $wordsPerLine /tmp/testdata"
    Process(command, path) !
    
    "/tmp/testdata"
  }
  
  /**
   * Generates a kronecker graph with the given parameters and
   * returns the path to the file.
   */
  def generateGraph(iterations: Int, v00: Double, v01: Double, v10: Double, v11: Double, runOnCluster: Boolean) : String = {

    if(runOnCluster) {
      getGraphS3(iterations, v00, v01, v10, v11)
    }
    else {
      val path = new java.io.File(bdgsPath + "Graph_datagen")

      val outputPath = s"/tmp/graphgen/${iterations}-${v00}-${v01}-${v10}-${v11}"
      val outputFile = s"${outputPath}/graph"

      // Only generate the file if it does not exist yet.
      if(!Files.exists(Paths.get(outputFile))) {
        Process(Seq("mkdir", "-p", outputPath)) #&&
          Process(Seq("./gen_kronecker_graph", s"-o:$outputFile", s"-m:$v00 $v01; $v10 $v11", s"-i:$iterations"), path) !
      }

      outputFile
    }

  }

  def getGraphS3(iterations: Int, v00: Double, v01: Double, v10: Double, v11: Double) : String = {
    val keyPath = s"graphgen/${iterations}-${v00}-${v01}-${v10}-${v11}/graph"
    val s3FileUrl = s"${s3BucketLink}graphgen/${iterations}-${v00}-${v01}-${v10}-${v11}/graph"
    if(!validateS3Path(keyPath)) {
      if(uploadFileToS3(generateGraph(iterations, v00, v01, v10, v11, false), keyPath)) {
        println(s"Uploaded File: $keyPath to Bucket: $s3BucketName")
      }
    }
    println(s"Fetching file from S3: $s3FileUrl")
    s3FileUrl
  }

  /**
   * Generates a Seq with n lines with two char each in the fashion:
   * AB
   * BC
   * CD
   * ...
   * YZ
   * YZ
   * 
   * Returns a local sequence.
   */
  def generateUniqueChars(iterations: Int) : Seq[String] = {
    var startChar = 'a'
    val list = scala.collection.mutable.Buffer[String]()
    for(i <- 0 until iterations) {
      val str = "" + (startChar + i).toChar + (startChar + i + 1).toChar
      list += str
    }
    // Repeat last line, so that only one unique char
    val str = "" + (startChar + iterations - 1).toChar + (startChar + iterations).toChar
    list += str
    
    list
  }
}
