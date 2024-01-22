package com.szubd.rsp

import org.apache.spark.{SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.net.URI

object MergeRSPWithUrl4 {
  val spark: SparkSession = SparkSession.builder().config(new SparkConf().setMaster("yarn")).getOrCreate()
  def main(args: Array[String]): Unit = {
    assert(args.length == 4, "args.length != 4")
    if (args(3) != "1" && args(3) != "2" && args(3) != "3") {
      println("args(3) is not 1 or 2 or 3")
      System.exit(1)
    }
    val tmpPath = args(0)
    println(args(1))
    val fileListUrl = args(1)
    val fileListString = spark.read.text(fileListUrl).rdd.first().getString(0)
    println(fileListString)
    val fileList: Array[String] = fileListString.split(":")
    for (j <- 0 until fileList.length) {
      val paths: Array[String] = fileList(j).split(",").map(p => tmpPath + "/" + p)
      val parquetFileDF = readOnce(args(3), paths)
      println("partitioncount:" + parquetFileDF.rdd.getNumPartitions)
      if (args(3) == "1") {
        parquetFileDF.repartition(args(2).toInt).write.parquet(tmpPath + "/global-rsp-block-" + j)
      } else {
        parquetFileDF.repartition(args(2).toInt).write.text(tmpPath + "/global-rsp-block-" + j)
      }
    }
  }

  def readOnce(fileType: String, path: Array[String]) : DataFrame = {
    if (fileType == "1") {
      spark.read.parquet(path:_*)
    } else if (fileType == "2") {
      spark.read.csv(path:_*)
    } else {
      spark.read.text(path:_*)
    }
  }
}
