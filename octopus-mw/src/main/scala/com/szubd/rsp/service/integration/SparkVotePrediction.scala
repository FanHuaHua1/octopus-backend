package com.szubd.rsp.service.integration

import org.apache.spark.SparkConf
import org.apache.spark.logo.etl.wrappers.BasicWrappers
import org.apache.spark.logo.ml.classification.{ClassificationJob, DecisionTree, LogisticRegression, LogoClassifier, RandomForest}
import smile.classification.{SoftClassifier, DecisionTree => smileDT, LogisticRegression => smileLR, RandomForest => smileRF}
import org.apache.spark.rdd.RDD
import org.apache.spark.rsp.RspRDD
import org.apache.spark.sql.RspContext.SparkSessionFunc
import org.apache.spark.sql.SparkSession
import org.springframework.scheduling.annotation.Async
import smile.data.Tuple
import smile.validation.metric.Accuracy

import scala.collection.mutable.{Map => MutablMap}
/**
 * @Author Lingxiang Zhao
 * @Date 2023/11/25 15:22
 * @desc
 */
object SparkVotePrediction {
  private val predictBlockPath = "E:/expdatas/datas/clf.parquet"
  //def run[M <: LogoClassifier[M]](modelRdd: RDD[(M, Double)], algo:String): Unit = {
  def run[M <: LogoClassifier[M]](modelS: List[(SoftClassifier[_ >: Tuple with Array[Double]], Double)], algo:String): Unit = {
    val sparkconf = new SparkConf().setAppName("Test_Smile").setMaster("local[*]")
    val spark = SparkSession
      .builder()
      .config(sparkconf)
      .getOrCreate()
    println("------------环境配置成功---------------")
    val frame = spark.rspRead.parquet(predictBlockPath)
    val modelRdd: RDD[(SoftClassifier[_ >: Tuple with Array[Double]], Double)] = spark.sparkContext.makeRDD(modelS)
    val predictWithIndex: RDD[((Array[Int], Array[Array[Double]]), Long)] = BasicWrappers.toMatrixRDD(frame.rdd).zipWithIndex()
    val predicts = predictWithIndex.map(item => (item._2, item._1, item._1._1.length))
    val prediction = algo match {
      case "LR" => modelRdd.cartesian(predicts).map(
        item => (item._2._1, new LogisticRegression().predictor(item._1._1.asInstanceOf[smileLR], item._2._2._2))
      ).groupBy(_._1)
      case "RF" => modelRdd.cartesian(predicts).map(
        item => (item._2._1, new RandomForest().predictor(item._1._1.asInstanceOf[smileRF], item._2._2._2))
      ).groupBy(_._1)
      case "DT" => modelRdd.cartesian(predicts).map(
        item => (item._2._1, new DecisionTree().predictor(item._1._1.asInstanceOf[smileDT], item._2._2._2))
      ).groupBy(_._1)
    }
    val sizeRDD = predicts.map(item => (item._1, item._3))
    val rspPredict = prediction.join(sizeRDD).map(votePrediction)
    val indexedLabels = predictWithIndex.map(item => (item._2, item._1._1))
    val rspAcc = rspPredict.join(indexedLabels).map(
      item => (Accuracy.of(item._2._1, item._2._2), item._2._1.length)
    )
    val acc = rspAcc.map(item => item._1 * item._2).sum / rspAcc.map(_._2).sum
    printf("Accuracy: %f\n", acc)
  }

  def votePrediction(param: (Long, (Iterable[(Long, Array[Int])], Int))): (Long, Array[Int]) = {
    val labels = param._2._1.map(_._2).toArray
    val result = new Array[Int](param._2._2)
    val members = labels.length
    val counts = MutablMap[Int, Int]()
    for (i <- 0 until param._2._2) {
      for (m <- 0 until members) {
        counts(labels(m)(i)) = counts.getOrElse(labels(m)(i), 0) + 1
      }
      result(i) = counts.maxBy(_._2)._1
      counts.clear()
    }

    (param._1, result)
  }

}
