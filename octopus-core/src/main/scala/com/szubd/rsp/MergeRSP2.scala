package com.szubd.rsp

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.RspContext._
object MergeRSP2 {
  val spark: SparkSession = SparkSession.builder().config(new SparkConf().setMaster("yarn")).getOrCreate()

  def main(args: Array[String]): Unit = {
    assert(args.length == 4, "args.length != 4")
    if(args(3) != "1" && args(3) != "2" && args(3) != "3") {
      println("args(3) is not 1 or 2 or 3")
      System.exit(1)
    }
    val tmpPath = args(0)
    val arr = new Array[String](args(2).toInt)
    for (i <- 0 until args(2).toInt) {
      arr(i) = tmpPath + "/block-" + i
    }
    val fileType = args(3)
    println(args(1))
    if (fileType == "1") {
      val frame = spark.rspRead.parquet(arr: _*)
      val pn = frame.rdd.getNumPartitions
      val inc = pn / 3
      println(pn)
      println("================")
      val countAndPartitionId: RDD[(Int, Int)] = frame.rdd.mapPartitionsWithIndex(
        (idx, f) => Iterator.single((f.count(_ => true), idx))
      )
      val sorted: Array[(Int, Int)] = countAndPartitionId.collect().sorted
      frame.rdd.mapPartitions(f => Iterator.single(f.count(_)))
      for (i <- 0 until (pn / 3)) {
        spark.createDataFrame(frame.rdd.getSubPartitions(Array(i, i+inc, i+inc+inc)), frame.schema).coalesce(1).write.parquet(tmpPath + "/block--" + i)
      }
    } else if (fileType == "2") {
      spark.read.csv(tmpPath)
    } else {
      spark.read.text(tmpPath).write.text(args(1))
    }
  }

}
