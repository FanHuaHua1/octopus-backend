package com.szubd.rsp.service.integration

import org.apache.spark.SparkConf
import org.apache.spark.logo.etl.wrappers.BasicWrappers
import smile.classification.{SoftClassifier, DecisionTree => smileDT, LogisticRegression => smileLR, RandomForest => smileRF}
import org.apache.spark.rdd.RDD
import org.apache.spark.rsp.RspRDD
import org.apache.spark.sql.RspContext.SparkSessionFunc
import org.apache.spark.sql.SparkSession
import org.springframework.scheduling.annotation.Async
import smile.clustering.{KMeans}
import smile.data.Tuple
import smile.validation.metric.Accuracy
import org.apache.spark.SparkContext
import org.apache.spark.logo.ml.clustering.bmutils.LOGOBisectingKMeans

/**
 * @Author Lingxiang Zhao
 * @Date 2023/11/25 15:22
 * @desc
 */
object SparkClusterPurity {
  private val predictBlockPath = "/home/zhaolingxiang/octopus-code/clfTestBlock.parquet2"
  //def run[M <: LogoClassifier[M]](modelRdd: RDD[(M, Double)], algo:String): Unit = {
  def run(res:Any, algo:String): String = {
    val sparkconf = new SparkConf().setAppName("Test_Smile").setMaster("local[*]")
      .set("spark.dynamicAllocation.minExecutors", "1")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.shuffle.service.enabled", "true")
    val spark = SparkSession
      .builder()
      .config(sparkconf)
      .getOrCreate()
    println("------------环境配置成功---------------")
    val frame = spark.rspRead.parquet(predictBlockPath)
    val predictRdd: RDD[(Array[Int], Array[Array[Double]])] = BasicWrappers.toMatrixRDD(frame.rdd)
    val puritySum = algo match {
      case "kmeans" => predictRdd.map(item => purity1(res.asInstanceOf[KMeans], item)).mean()
      case "bisectingkmeans" => predictRdd.map(item => purity2(res.asInstanceOf[LOGOBisectingKMeans], item)).mean()
    }
    printf("Purity: %.4f\n", puritySum)
    puritySum.formatted("%.4f")
  }

  def purity1(model: KMeans, test: (Array[Int], Array[Array[Double]])): Double = {
    val predictions = test._2.map(model.predict)
    var p = predictions.zip(test._1)
      .count(r => r._1 == r._2)
      .toDouble / predictions.length
    if (p<0.5) {
      p = 1 - p
    }
    p
  }

  def purity2(model: LOGOBisectingKMeans, test: (Array[Int], Array[Array[Double]])): Double = {
    val predictions = test._2.map(model.predict)
    var p = predictions.zip(test._1)
      .count(r => r._1 == r._2)
      .toDouble / predictions.length
    if (p<0.5) {
      p = 1 - p
    }
    p
  }
}
