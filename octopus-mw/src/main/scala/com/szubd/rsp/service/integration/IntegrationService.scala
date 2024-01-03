package com.szubd.rsp.service.integration

import com.szubd.rsp.integration.IntegrationDubboService
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

import java.util
import java.util.HashMap
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
    val objects: List[Object] = trieMap(jobId)
    val modelCount = modelCountTrieMap(jobId)
    logger.info("{}算法开始跨域GO集成，模型数量：{}，算法参数：{}", algoName, String.valueOf(modelCount), args)
    val itegrationStartTime = System.currentTimeMillis()
    algoType match {
      case "clf" => clfIntegrate(objects, algoName, modelCount, args, jobId)
      case "clt" => cltIntegrate(objects, algoName, modelCount, args, jobId)
      case "fpg" => fpgIntegrate(objects, algoName, modelCount, args, jobId)
    }
    val goDuration = System.currentTimeMillis() - itegrationStartTime + "ms"
    jobService.updateJobArgs(jobId, "goTimeConsuming", goDuration)
    jobService.syncInDB(jobId)
    logger.info("GO耗时：{}", goDuration)
    trieMap.remove(jobId)
    modelCountTrieMap.remove(jobId)
  }



  def clfIntegrate(models: List[Object], algoName: String, modelCount: Int, args:String, jobId: Int): Unit = {
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
    SparkVotePrediction.run(res: List[(SoftClassifier[_ >: Tuple with Array[Double]], Double)], algoName)
    //res.foreach(f => println("===============]>>" + f._1 + " " + f._2))
  }

  def cltIntegrate(models: List[Object], algoName: String, modelCount: Int, args:String, jobId: Int): Unit  = {
    val formattedModels = models.asInstanceOf[List[Array[Array[Double]]]]
    val flatCenters = formattedModels.flatMap(f => f.iterator).toArray
    val res : Array[Array[Double]] = algoName match {
      case "kmeans" => smileKMeans.fit(flatCenters, 2).centroids
      case "bisectingkmeans" => {
        val bm = new LOGOBisectingKMeans(2, 2)
        bm.clustering(flatCenters)
        bm.centroids
      }
    }
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

