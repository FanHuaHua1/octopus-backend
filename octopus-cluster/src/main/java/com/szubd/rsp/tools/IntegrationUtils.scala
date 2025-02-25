package com.szubd.rsp.tools

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import javafx.util.Pair
import smile.classification.{DecisionTree, LogisticRegression, RandomForest, SVM}

import scala.reflect.{ClassTag, classTag}

/**
 * @Author Lingxiang Zhao
 * @Date 2023/10/10 12:02
 * @desc
 */
object IntegrationUtils {

//  def getModels[T:ClassTag](path: String): T = {
//    val sparkconf = new SparkConf().setAppName("Integration").setMaster("local[*]")
//    val sc = new SparkContext(sparkconf)
//    val value1: RDD[T] = sc.objectFile(path)
//    value1.collect()(1)
//  }

//  def getModels(path: String, algoType: String, algoName: String): Object = {
//    //    println("algoType: " + algoType + ", algoName: " + algoName)
//    val sparkconf = new SparkConf().setAppName("Integration").setMaster("local[*]")
//    val sc = new SparkContext(sparkconf)
//    //val value1: RDD[Pair[DecisionTree,Double]] =
//    val array: Array[Pair[DecisionTree, Double]] = sc.objectFile(path).collect()
//    array.foreach(f => println("===============]>>" + f.getKey + " " + f.getValue))
//    array
//  }

  def getModels(sc: SparkContext, path: String, algoType: String, algoName: String): (Object, Int) = {
    algoType match {
      case "clf" =>
        val rdd: RDD[(Any, Double)] = sc.objectFile(path)
        val modelCount = rdd.getNumPartitions
        //val arr: Array[(Any, Double)] = sc.objectFile(path).collect()
        println("==================modelCount " + modelCount)
        val models: Array[(Any, Double)] = rdd.collect()
        algoName match {
          case "RF" => models.asInstanceOf[Array[(RandomForest, Double)]]
          case "DT" => models.asInstanceOf[Array[(DecisionTree, Double)]]
          case "LR" => models.asInstanceOf[Array[(LogisticRegression, Double)]]
          case "SVM" => models.asInstanceOf[Array[(SVM[Array[Double]], Double)]]
        }
        (models, modelCount)
      case "clt" =>
        val modelsRdd: RDD[Array[Double]] = sc.objectFile(path)
        val modelCount = modelsRdd.getNumPartitions
        val models: Array[Array[Double]] = modelsRdd.collect()
        (models, modelCount)
      case "fpg" =>
        val modelsRdd: RDD[(String, Int)] = sc.objectFile(path)
        val modelCount = modelsRdd.getNumPartitions
        val models: Array[(String, Int)] = modelsRdd.collect()

//        val models:Array[(String, Int)] = algoName match {
//          case "Vote" => sc.objectFile(path).collect()
//        }
      (models, modelCount)
    }
  }

  def getModels(path: String, algoType: String, algoName: String): (Object, Int) = {
    println("algoType: " + algoType + ", algoName: " + algoName)
    val sparkconf = new SparkConf().setAppName("Integration").setMaster("local[*]")
    val sc = new SparkContext(sparkconf)
    var models: Object = null
    var modelsCount = 0
    try {
     //models = getModels(sc, path, algoType, algoName)
     val (modelsTemp, modelsCountTemp) = getModels(sc, path, algoType, algoName)
      models = modelsTemp
      modelsCount = modelsCountTemp
      println("================modelsCount: " + modelsCount)
    } catch {
      case e: Exception => println("getModels error: " + e.getMessage)
    } finally {
      sc.stop()
    }
    (models, modelsCount)
  }

}
