package com.szubd.rsp.service.integration

import com.alibaba.fastjson.JSON
import com.szubd.rsp.integration.IntegrationDubboService
import com.szubd.rsp.job.JobInfo
import com.szubd.rsp.service.job.JobService
import com.typesafe.scalalogging.Logger
import org.apache.commons.lang3.StringUtils
import org.apache.dubbo.config.annotation.DubboService
import org.apache.hadoop.mapreduce.v2.api.records.JobId
import org.apache.spark.logo.ml.clustering.bmutils.LOGOBisectingKMeans
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.annotation.{Async, Scheduled}
import org.springframework.stereotype.{Component, Service}
import smile.classification.{Classifier, DecisionTree, LogisticRegression, RandomForest, SVM, SoftClassifier}
import smile.clustering.{KMeans => smileKMeans}
import smile.data.Tuple

import scala.collection.JavaConverters._
import java.io.{FileOutputStream, FileWriter}
import java.util.Comparator
import java.util.concurrent.Executor
import scala.collection.concurrent.TrieMap
import scala.collection.parallel.ParMap
import scala.collection.parallel.immutable.ParSeq


/**
 * @Author Lingxiang Zhao
 * @Date 2023/10/11 13:57
 * @desc
 */
@DubboService
@Component
class IntegrationService @Autowired()(jobService: JobService, mwTaskExecutor: Executor)  extends IntegrationDubboService{

  private val logger = LoggerFactory.getLogger(classOf[IntegrationService])
  private val trieMap = TrieMap[Int, List[Object]]()
  private val pathTrieMap = TrieMap[Int, List[String]]()
  private val modelCountTrieMap = TrieMap[Int, Int]()

  override def recordModels(jobId: Int, models: Any): Unit = {
    logger.info(" [IntegrationService] models record from job: {}", jobId)
    val modelAndPartitionCount: (Object, Int) = models.asInstanceOf[(Object, Int)]
    val parentJobId = jobService.getParentId(jobId)
    //val newList: List[Object] = trieMap.getOrElse(parentJobId, List[Object]()) :+ models.asInstanceOf[Object]
    val newList: List[Object] = trieMap.getOrElse(parentJobId, List[Object]()) :+ modelAndPartitionCount._1
    trieMap.update(parentJobId, newList)
    modelCountTrieMap.update(parentJobId, modelCountTrieMap.getOrElse(parentJobId, 0) +  modelAndPartitionCount._2)
  }

  override def recordModelPaths(jobId: Int, path: String): Unit = {
    logger.info(" [IntegrationService] models record from job: {}", jobId)
    val parentJobId = jobService.getParentId(jobId)
    //val newList: List[Object] = trieMap.getOrElse(parentJobId, List[Object]()) :+ models.asInstanceOf[Object]
    val newList: List[String] = pathTrieMap.getOrElse(parentJobId, List[String]()) :+ path
    pathTrieMap.update(parentJobId, newList)
    //modelCountTrieMap.update(parentJobId, modelCountTrieMap.getOrElse(parentJobId, 0) +  modelAndPartitionCount._2)
  }

  override def checkAndIntegrate(jobId: Int, algoType: String, algoName: String, runArgs: String): Unit = {
    val args = jobService.getJobInfo(jobService.getParentId(jobId)).getArgs
    val num = args.get("subJobNum").toInt
    logger.info(" [IntegrationService] subjob left: {}", num)
    if(num == 0){
      mwTaskExecutor.execute(new Runnable {
        override def run(): Unit = {
          integrate(jobService.getParentId(jobId), algoType, algoName, runArgs)
        }
      })
      //integrate(jobService.getParentId(jobId), algoType, algoName, runArgs)
    }
  }
  def integrate(jobId: Int, algoType: String, algoName: String, args: String): Unit = {
    val objects: List[String] = pathTrieMap(jobId)
    val itegrationStartTime = System.currentTimeMillis()
    val res = algoType match {
      case "clf" => clfIntegrate2(objects, algoName, 0, args, jobId)
      case "clt" => cltIntegrate(objects, algoName, 0, args, jobId)
      case "fpg" => fpgIntegrate(objects, algoName, 0, args, jobId)
    }
    val goDuration = ((System.currentTimeMillis() - itegrationStartTime) * 0.001).formatted("%.2f")
    jobService.updateJobArgs(jobId, "goTimeConsuming", goDuration)
    jobService.syncInDB(jobId)
    logger.info("GO耗时：{}", goDuration)
    val algoJobInfo = jobService.getJobInfoById(jobId)
    val subJobInfos = jobService.getJobInfosByParentId(jobId)
    subJobInfos.sort(new Comparator[JobInfo] {
      override def compare(o1: JobInfo, o2: JobInfo): Int = {
        val o1ip = o1.getRunningHost
        val o2ip = o2.getRunningHost
        if(o1ip.equals("192.168.0.67")){
          return 1
        } else if(o1ip.equals("192.168.0.120")){
          return -1;
        } else {
          if(o2ip.equals("192.168.0.67")){
            return -1;
          } else {
            return 1;
          }
        }
      }
    })
    val fw = new FileWriter("/home/zhaolingxiang/expLog", true)
    val subJobInfosInScala = subJobInfos
      .asScala
      .toList
      .map(str =>JSON.parse(str.getArgsJsonString).asInstanceOf[java.util.Map[String, String]])
    println(subJobInfosInScala)
    val loTimeConsumingList = subJobInfosInScala.map(info => info.get("loTimeConsuming").toDouble)
    val loString = loTimeConsumingList.mkString(" ") + " " + loTimeConsumingList.max + " "
    println(loString)
    fw.append(loString)
    val mTTimeConsumingList = subJobInfosInScala.map(info => info.get("modelsTransferTimeConsuming").toDouble)
    val mTString = mTTimeConsumingList.mkString(" ") + " " + mTTimeConsumingList.max + " "
    fw.append(mTString)
    fw.append(goDuration)
    fw.append(" " + res + "\n")
    fw.close()
    trieMap.remove(jobId)
    modelCountTrieMap.remove(jobId)
    jobService.reduceJobCountDown(jobId)
  }


//  def integrate(jobId: Int, algoType: String, algoName: String, args: String): Unit = {
//    val objects: List[Object] = trieMap(jobId)
//    val modelCount = modelCountTrieMap(jobId)
//    logger.info("{}算法开始跨域GO集成，模型数量：{}，算法参数：{}", algoName, String.valueOf(modelCount), args)
//    val itegrationStartTime = System.currentTimeMillis()
//    val res = algoType match {
//      case "clf" => clfIntegrate(objects, algoName, modelCount, args, jobId)
//      case "clt" => cltIntegrate(objects, algoName, modelCount, args, jobId)
//      case "fpg" => fpgIntegrate(objects, algoName, modelCount, args, jobId)
//    }
//    val goDuration = ((System.currentTimeMillis() - itegrationStartTime) * 0.001).formatted("%.2f")
//    jobService.updateJobArgs(jobId, "goTimeConsuming", goDuration)
//    jobService.syncInDB(jobId)
//    logger.info("GO耗时：{}", goDuration)
//    val algoJobInfo = jobService.getJobInfoById(jobId)
//    val subJobInfos = jobService.getJobInfosByParentId(jobId)
//    subJobInfos.sort(new Comparator[JobInfo] {
//      override def compare(o1: JobInfo, o2: JobInfo): Int = {
//        val o1ip = o1.getRunningHost
//        val o2ip = o2.getRunningHost
//        if(o1ip.equals("192.168.0.67")){
//          return 1
//        } else if(o1ip.equals("192.168.0.120")){
//          return -1;
//        } else {
//          if(o2ip.equals("192.168.0.67")){
//            return -1;
//          } else {
//            return 1;
//          }
//        }
//      }
//    })
//    val fw = new FileWriter("/home/zhaolingxiang/expLog", true)
//    val subJobInfosInScala = subJobInfos
//      .asScala
//      .toList
//      .map(str =>JSON.parse(str.getArgsJsonString).asInstanceOf[java.util.Map[String, String]])
//    println(subJobInfosInScala)
//    val loTimeConsumingList = subJobInfosInScala.map(info => info.get("loTimeConsuming").toDouble)
//    val loString = loTimeConsumingList.mkString(" ") + " " + loTimeConsumingList.max + " "
//    println(loString)
//    fw.append(loString)
//    val mTTimeConsumingList = subJobInfosInScala.map(info => info.get("modelsTransferTimeConsuming").toDouble)
//    val mTString = mTTimeConsumingList.mkString(" ") + " " + mTTimeConsumingList.max + " "
//    fw.append(mTString)
//    fw.append(goDuration)
//    fw.append(" " + res + "\n")
//    fw.close()
//    trieMap.remove(jobId)
//    modelCountTrieMap.remove(jobId)
//    jobService.reduceJobCountDown(jobId)
//  }



  def clfIntegrate(models: List[Object], algoName: String, modelCount: Int, args:String, jobId: Int): String = {
    val itegrationStartTime = System.nanoTime()
    val formattedModels =
    algoName match {
      case "RF" =>  models.asInstanceOf[List[Array[(RandomForest, Double)]]]
      case "DT" =>  models.asInstanceOf[List[Array[(DecisionTree, Double)]]]
      case "LR" => models.asInstanceOf[List[Array[(LogisticRegression, Double)]]]
      //case "SVM" =>  models.asInstanceOf[List[Array[(SVM[Array[Double]], Double)]]]
    }
    val tuples = formattedModels.flatMap(f => f.iterator)
    val factors = tuples.map(_._2).sorted
    val mcount = tuples.length
    printf("Model count: %d\n", mcount)
    var tcount = (mcount * 0.05).toInt
    if (tcount < 1) {
      tcount = 1
    }
    printf("Tail count: %d\n", tcount)
    val (minAcc, maxAcc) = (factors(tcount), factors(mcount - tcount - 1))
    printf("Score range: (%f, %f)\n", minAcc, maxAcc)
    val res: List[(SoftClassifier[_ >: Tuple with Array[Double]], Double)] = tuples.filter(item => minAcc <= item._2 && item._2 <= maxAcc).map(item => (item._1, item._2))
    val goDuration = System.nanoTime() - itegrationStartTime
    goDuration + " " + SparkVotePrediction.run(res: List[(SoftClassifier[_ >: Tuple with Array[Double]], Double)], algoName)
    //res.foreach(f => println("===============]>>" + f._1 + " " + f._2))
  }

  def clfIntegrate2(modelPath: List[String], algoName: String, modelCount: Int, args:String, jobId: Int): String = {
    val itegrationStartTime = System.nanoTime()
//    val formattedModels =
//      algoName match {
//        case "RF" =>  models.asInstanceOf[List[Array[(RandomForest, Double)]]]
//        case "DT" =>  models.asInstanceOf[List[Array[(DecisionTree, Double)]]]
//        case "LR" => models.asInstanceOf[List[Array[(LogisticRegression, Double)]]]
//        //case "SVM" =>  models.asInstanceOf[List[Array[(SVM[Array[Double]], Double)]]]
//      }
//    val tuples = formattedModels.flatMap(f => f.iterator)
//    val factors = tuples.map(_._2).sorted
//    val mcount = tuples.length
//    printf("Model count: %d\n", mcount)
//    var tcount = (mcount * 0.05).toInt
//    if (tcount < 1) {
//      tcount = 1
//    }
//    printf("Tail count: %d\n", tcount)
//    val (minAcc, maxAcc) = (factors(tcount), factors(mcount - tcount - 1))
//    printf("Score range: (%f, %f)\n", minAcc, maxAcc)
//    val res: List[(SoftClassifier[_ >: Tuple with Array[Double]], Double)] = tuples.filter(item => minAcc <= item._2 && item._2 <= maxAcc).map(item => (item._1, item._2))

    val str = SparkVotePrediction2.run(modelPath, algoName)
    val goDuration = System.nanoTime() - itegrationStartTime
    goDuration + " " + str
    //res.foreach(f => println("===============]>>" + f._1 + " " + f._2))
  }

  def cltIntegrate(models: List[Object], algoName: String, modelCount: Int, args:String, jobId: Int): String  = {
    val formattedModels = models.asInstanceOf[List[Array[Array[Double]]]]
    val flatCenters = formattedModels.flatMap(f => f.iterator).toArray
    var tol: Double = 1.0E-4
//    val res : Array[Array[Double]] = algoName match {
//      case "kmeans" => smileKMeans.fit(flatCenters, 2).centroids
//      case "bisectingkmeans" => {
//        val bm = new LOGOBisectingKMeans(2, 2)
//        bm.clustering(flatCenters)
//        bm.centroids
//      }
//    }
    val res = algoName match {
      case "kmeans" => smileKMeans.fit(flatCenters, 2, 100, tol)
      case "bisectingkmeans" => {
        val bm = new LOGOBisectingKMeans(2, 10)
        bm.clustering(flatCenters)
        bm
      }
    }
    SparkClusterPurity.run(res, algoName)
  }



  def fpgIntegrate(models: List[Object], algoName: String, modelCount: Int, args:String, jobId: Int): Unit = {
    val formattedModels = models.asInstanceOf[List[Array[(String, Int)]]]
    val parRes: ParSeq[(String, (Int, Int))] = formattedModels.par.flatMap(oneModelRes => {
      //val formatedOneModelRes: Array[(String, (Int, Int))] = oneModelRes.map(elem => (elem._1, (elem._2, 1)))
      val formatedOneModelRes = oneModelRes.map(elem => (elem._1, (elem._2, 1))).iterator
      formatedOneModelRes
    })
    val goRes: ParMap[String, Double] = parRes.groupBy(_._1)
      .mapValues(_.map(_._2).reduce((a, b) => (a._1 + b._1, a._2 + b._2)))
      .filter(elem => elem._2._2 >= modelCount * 0.5)
      .mapValues(elem => elem._1 * 1.0 / elem._2)
    goRes.foreach(f => logger.info(f._1 + " " + f._2))
  }
}

