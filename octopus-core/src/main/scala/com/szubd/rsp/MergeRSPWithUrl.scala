package com.szubd.rsp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}

import java.net.URI

object MergeRSPWithUrl {
  val sparkConf: SparkConf = new SparkConf().setMaster("yarn").setAppName("merge")
  //val sc = new SparkContext(sparkConf)
  val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  def main(args: Array[String]): Unit = {
    assert(args.length == 4, "args.length != 4")
    if(args(3) != "1" && args(3) != "2" && args(3) != "3") {
      println("args(3) is not 1 or 2 or 3")
      System.exit(1)
    }
    val tmpPath = args(0)
    println(args(1))
    val fileListUrl = args(1)
//    val uri = new URI(fileListUrl)
//    val conf = new Configuration
//    val fs = FileSystem.get(uri, conf, "hdfs")
//    val is: FSDataInputStream = fs.open(new Path(fileListUrl))
//    val fileListString = scala.io.Source.fromInputStream(is).mkString
//    is.close()
    val fileListString = spark.read.text(fileListUrl).rdd.first().getString(0)
    println(fileListString)
    val fileList: Array[String] = fileListString.split(":")
    import  spark.implicits._
    val ds = spark.createDataset(fileList)
    //val fileListRdd: RDD[(String, Long)] = sc.makeRDD(fileList).zipWithIndex()
    val fileListRdd: RDD[(String, Long)] = ds.rdd.zipWithIndex()
    fileListRdd.foreach( fileListElem => {
      val paths: Array[String] = fileListElem._1.split(",")
      var parquetDFArray = new Array[DataFrame](paths.length)
      for (i <- 0 until paths.length) {
        println("path:" + tmpPath + "/" + paths(i))
        val parquetFileDF = readOnce(tmpPath + "/" + paths(i), args(3))
        parquetDFArray(i) = parquetFileDF
      }
      val parquetFileDF = parquetDFArray.reduce((a, b) => a.union(b))
      if (args(3) == "1") {
        parquetFileDF.repartition(args(2).toInt).write.parquet(tmpPath + "/global-rsp-block-" + fileListElem._2)
      } else {
        parquetFileDF.repartition(args(2).toInt).write.text(tmpPath + "/global-rsp-block-" + fileListElem._2)
      }
    } )
  }

  def readOnce(path: String, fileType: String) : DataFrame = {
    if (fileType == "1") {
      spark.read.parquet(path)
    } else if (fileType == "2") {
      spark.read.csv(path)
    } else {
      spark.read.text(path)
    }
  }
}
