package com.szubd.rsp

import org.apache.spark.{SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.net.URI

object MergeRSPWithUrl2 {
  val spark: SparkSession = SparkSession.builder().config(new SparkConf().setMaster("yarn")).getOrCreate()
  def main(args: Array[String]): Unit = {
    assert(args.length == 4, "args.length != 4")
    if(args(3) != "1" && args(3) != "2" && args(3) != "3") {
      println("args(3) is not 1 or 2 or 3")
      System.exit(1)
    }
    val tmpPath = args(0)
    println(args(1))
    val fileListUrl = args(1)
    val fileListString = spark.read.text(fileListUrl).rdd.first().getString(0)
    println(fileListString)
    val fileList: Array[String] = fileListString.split(":")
    //var totalParquetDFArray = new Array[DataFrame](fileList.length)
    for (j <- 0 until fileList.length) {
      val paths: Array[String] = fileList(j).split(",")
      var parquetDFArray = new Array[DataFrame](paths.length)
      for (i <- 0 until paths.length) {
        println("path:" + tmpPath + "/" + paths(i))
        val parquetFileDF = readOnce(tmpPath + "/" + paths(i), args(3))
        parquetDFArray(i) = parquetFileDF
      }
      val parquetFileDF = parquetDFArray.reduce((a, b) => a.union(b)).repartition(1)
      ///totalParquetDFArray(j) = parquetFileDF
      if (args(3) == "1") {
        parquetFileDF.repartition(args(2).toInt).write.parquet(tmpPath + "/global-rsp-block-" + j)
      } else {
        parquetFileDF.repartition(args(2).toInt).write.text(tmpPath + "/global-rsp-block-" + j)
      }
    }
    //totalParquetDFArray.reduce((a, b) => a.union(b)).write.parquet(tmpPath + "/global-rsp-block")
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
