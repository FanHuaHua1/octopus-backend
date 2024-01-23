package com.szubd.rsp

import org.apache.spark.SparkConf
import org.apache.spark.rsp.RspRDD
import org.apache.spark.sql.RspContext.NewRDDFunc
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object ToRsp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("yarn")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    var parquetFileDF: DataFrame = null
    if (args(3) == "1") {
      parquetFileDF = spark.read.parquet(args(0))
    } else if (args(3) == "2") {
      parquetFileDF = spark.read.csv(args(0))
    } else if (args(3) == "3") {
      parquetFileDF = spark.read.text(args(0))
      val textRDD = parquetFileDF.rdd
      val textRspRDD = textRDD.toRSP(args(2).toInt).map(row => row.getString(0))
      textRspRDD.saveAsTextFile(args(1))
      return
    } else {
      println("args(3) is not 1 or 2")
      System.exit(1)
    }
    //parquetFileDF = spark.read.parquet(args(0))
    val rdd3 = parquetFileDF.rdd
    val value: RspRDD[Row] = rdd3.toRSP(args(2).toInt)
    val frame: DataFrame = spark.createDataFrame(value, parquetFileDF.schema)
    frame.write.parquet(args(1))
  }
}
