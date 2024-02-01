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
object SparkVotePrediction2 {
  private val predictBlockPath = "/home/zhaolingxiang/octopus-code/clfTestBlock.parquet2"
  //def run[M <: LogoClassifier[M]](modelRdd: RDD[(M, Double)], algo:String): Unit = {
  def run[M <: LogoClassifier[M]](modelPath:List[String], algo:String): String = {
    val sparkconf = new SparkConf().setAppName("Test_Smile").setMaster("local[*]")
      .set("spark.dynamicAllocation.minExecutors", "1")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.shuffle.service.enabled", "true")
    val spark = SparkSession
      .builder()
      .config(sparkconf)
      .getOrCreate()
    println("------------环境配置成功---------------")
    //part1 - 数据传输
    val time1 = System.currentTimeMillis()
//    val rdd: RDD[(Any, Double)] = sc.objectFile(path)
    val sc = spark.sparkContext
    val rdd1 = sc.objectFile(modelPath(0))
    val rdd2 = sc.objectFile(modelPath(1))
    val rdd3 = sc.objectFile(modelPath(2))
    val modelRdd = rdd1.union(rdd2).union(rdd3).asInstanceOf[RDD[(Any, Double)]]
    val modelCount = modelRdd.getNumPartitions
    val count = modelRdd.count()
    val time2 = System.currentTimeMillis()
    val time1Str = ((time2 - time1) * 0.001).formatted("%.2f")
    //part2 - go
    algo match {
        case "RF" => modelRdd.asInstanceOf[RDD[(RandomForest, Double)]]
        case "DT" => modelRdd.asInstanceOf[RDD[(DecisionTree, Double)]]
        case "LR" => modelRdd.asInstanceOf[RDD[(LogisticRegression, Double)]]
      }
      val factors = modelRdd.map(_._2).collect().sorted
      val mcount = modelCount
      printf("Model count: %d\n", mcount)
      var tcount = (mcount * 0.05).toInt
      if (tcount < 1) {
        tcount = 1
      }
      printf("Tail count: %d\n", tcount)
      val (minAcc, maxAcc) = (factors(tcount), factors(mcount - tcount - 1))
      printf("Score range: (%f, %f)\n", minAcc, maxAcc)
    val valuedModels = modelRdd.filter(item => minAcc <= item._2 && item._2 <= maxAcc).map(item => (item._1, item._2))
    val count3 = valuedModels.count()
    val time3 = System.currentTimeMillis()
    val time2Str = ((time3 - time2) * 0.001).formatted("%.2f")
    //part3 - predict
    val frame = spark.rspRead.parquet(predictBlockPath)
    val predictWithIndex: RDD[((Array[Int], Array[Array[Double]]), Long)] = BasicWrappers.toMatrixRDD(frame.rdd).zipWithIndex()
    val predicts = predictWithIndex.map(item => (item._2, item._1, item._1._1.length))
    valuedModels.cartesian(predicts).map(
        item => (item._2._1, new LogisticRegression().predictor(item._1._1.asInstanceOf[smileLR], item._2._2._2))
      ).groupBy(_._1)
    val prediction = algo match {
      case "LR" => valuedModels.cartesian(predicts).map(
        item => (item._2._1, new LogisticRegression().predictor(item._1._1.asInstanceOf[smileLR], item._2._2._2))
      ).groupBy(_._1)
      case "RF" => valuedModels.cartesian(predicts).map(
        item => (item._2._1, new RandomForest().predictor(item._1._1.asInstanceOf[smileRF], item._2._2._2))
      ).groupBy(_._1)
      case "DT" => valuedModels.cartesian(predicts).map(
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
    printf("Accuracy: %.4f\n", acc)
    val time4 = System.currentTimeMillis()
    val time3Str = ((time4 - time3) * 0.001).formatted("%.2f")
    time1Str + " " + time2Str + " " + time3Str + " " + acc.formatted("%.4f")
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
