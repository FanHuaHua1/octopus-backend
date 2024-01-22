package com.szubd.rsp

import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.GroupFactory
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.ParquetReader.Builder
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.parquet.hadoop.example.GroupWriteSupport
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.MessageTypeParser


import java.net.URI

object MergeRSPWithUrl {

  def main(args: Array[String]): Unit = {
    assert(args.length == 4, "args.length != 4")
    if(args(3) != "1" && args(3) != "2" && args(3) != "3") {
      println("args(3) is not 1 or 2 or 3")
      System.exit(1)
    }
    val tmpPath = args(0)
    println(args(1))
    val fileListUrl = args(1)
    val spark: SparkSession = SparkSession.builder().config(new SparkConf().setMaster("yarn")).getOrCreate()
    val fileListString = spark.read.text(fileListUrl).rdd.first().getString(0)
    println(fileListString)
    val fileList: Array[String] = fileListString.split(":")
    import spark.implicits._
    val ds = spark.createDataset(fileList)
    //val fileListRdd: RDD[(String, Long)] = sc.makeRDD(fileList).zipWithIndex()
    val fileListRdd: RDD[(String, Long)] = ds.rdd.zipWithIndex()
    fileListRdd.collect().foreach(println)
    fileListRdd.foreach(fileListElem => {
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
    val spark: SparkSession = SparkSession.builder().config(new SparkConf().setMaster("yarn")).getOrCreate()
    if (fileType == "1") {
      spark.read.parquet(path)
    } else if (fileType == "2") {
      spark.read.csv(path)
    } else {
      spark.read.text(path)
    }
  }

}
