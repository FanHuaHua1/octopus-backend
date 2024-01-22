package com.szubd.rsp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{Partitioner, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.RspContext._

import scala.collection.mutable.ArrayBuffer
object MergeRSP3 {
  val spark: SparkSession = SparkSession.builder().config(new SparkConf().setMaster("yarn")).getOrCreate()

  def main(args: Array[String]): Unit = {
    assert(args.length == 4, "args.length != 4")
    if (args(3) != "1" && args(3) != "2" && args(3) != "3") {
      println("args(3) is not 1 or 2 or 3")
      System.exit(1)
    }
    val tmpPath = args(0)
    val fs = FileSystem.get(new Configuration())
    val fss = fs.listStatus(new Path(tmpPath))
    var parquetDFArray = new Array[DataFrame](fss.length)
    for (i <- 0 until fss.length) {
      val parquetFileDF = spark.rspRead.parquet(tmpPath + "/" + fss(i).getPath.getName)
      parquetDFArray(i) = parquetFileDF
    }
    val parNum = parquetDFArray(0).rdd.getNumPartitions
    val schema = parquetDFArray(0).schema
    val array: Array[RDD[(Int, Array[Row])]] = parquetDFArray.map(_.rdd.mapPartitionsWithIndex((idx, f) => Iterator.single((idx, f.toArray))))
    val value: RDD[Row] = array.reduce((a, b) => a.union(b)).partitionBy(new RSPPartitioner(parNum)).mapPartitions(f => {
      val array = ArrayBuffer[Row]()
      while (f.hasNext) {
        f.next()._2.copyToBuffer(array)
      }
      array.iterator
    })
    val fileType = args(3)
    println(args(1))
    if (fileType == "1") {
      val frame = spark.createDataFrame(value, schema)
      frame.write.parquet(args(1))
    } else if (fileType == "2") {
      spark.read.csv(tmpPath)
    } else {
      spark.read.text(tmpPath).write.text(args(1))
    }
  }


  //  def readOnce(path: String, fileType: String) : DataFrame = {
  //    if (fileType == "1") {
  //      spark.read.parquet(path)
  //    } else if (fileType == "2") {
  //      spark.read.csv(path)
  //    } else {
  //      spark.read.text(path)
  //    }
  //  }

  //自定义分区器,继承Partitioner
  class RSPPartitioner(numPartition: Int) extends Partitioner {
    //numPartitions代表分区的数目,可以利用构造函数传入,也可以设定为固定值
    override def numPartitions: Int = numPartition

    //依据输入内容计算分区id(就是属于哪个分区)
    override def getPartition(key: Any): Int = {
      //key的类型为Any,需要转换为String
      key.asInstanceOf[Int]
    }
  }
}
