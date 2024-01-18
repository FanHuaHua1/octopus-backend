package com.szubd.rsp.service.algo;

import com.szubd.rsp.algo.AlgoDubboService;
import com.szubd.rsp.constants.JobConstant;
import com.szubd.rsp.constants.RSPConstant;
import com.szubd.rsp.integration.IntegrationDubboService;
import com.szubd.rsp.job.JobDubboLogoService;
import com.szubd.rsp.job.JobDubboService;
import com.szubd.rsp.job.JobInfo;
import com.szubd.rsp.job.JobLogoInfo;
import com.szubd.rsp.service.job.JobUtils;
import com.szubd.rsp.tools.IntegrationUtils;
import com.szubd.rsp.user.UserDubboService;
import com.szubd.rsp.user.UserInfo;
import com.szubd.rsp.user.UserResource;
import com.szubd.rsp.user.UserResourceDubboService;
import org.apache.commons.lang3.StringUtils;
import org.apache.dubbo.config.annotation.DubboReference;
import org.apache.dubbo.config.annotation.DubboService;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import scala.reflect.ClassTag;
import smile.classification.DecisionTree;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.UUID;

@Component
@DubboService(async = true)
public class AlgoDubboServiceImpl implements AlgoDubboService {
    @Autowired
    private RSPConstant constant;
    @Autowired
    private JobConstant jobConstant;

    @DubboReference
    private JobDubboLogoService jobDubboLogoService;
    @DubboReference
    private IntegrationDubboService integrationDubboService;
    @DubboReference
    private JobDubboService jobDubboService;

    @DubboReference
    private UserDubboService userDubboService;
    @DubboReference
    private UserResourceDubboService userResourceDubboService;
    protected static final Logger logger = LoggerFactory.getLogger(AlgoDubboServiceImpl.class);

    @Async("taskExecutor")
    @Override
    public void toAlgo(int jobId,
                       String algoType,
                       String algoSubSetting,
                       String algoName,
                       String filename,
                       int executorNum,
                       int executorMemory,
                       int executorCores,
                       String args) {
        logger.info(Thread.currentThread().getName() + ": 正在发起调用算法执行调用");
        logger.info("当前线程池任务数: {}", Thread.currentThread().getName());
        logger.info("任务信息：{}", jobId + ", " + algoType + ", " + algoSubSetting + ", " + algoName + ", " + filename + ", " + executorNum + ", " + executorMemory + ", " + executorCores + ", 其他参数: " + String.join(",", args));
        String host;
        try {
            host = new URI(constant.url).getHost();
        } catch (URISyntaxException e) {
            host = "wrong";
        }
        JobInfo subAlgoJobInfo = new JobInfo(1, "algo", "RUNNING" , host, jobId);
        int subAlgoJobId = jobDubboService.createJob(subAlgoJobInfo);
        jobDubboService.updateJobArgs(subAlgoJobId, "数据集", filename);
        try {
            jobDubboService.updateJobArgs(subAlgoJobId, "ip", new URI(constant.url).getHost());
        } catch (URISyntaxException e) {
            jobDubboService.updateJobArgs(subAlgoJobId, "ip", "wrong");
        }
        jobDubboService.syncInDB(subAlgoJobId);
        try {
            String path = constant.url + constant.modelPrefix + UUID.randomUUID();
            SparkLauncher launcher = getCorrespondingLauncher(
                    algoType,
                    algoSubSetting,
                    algoName,
                    filename,
                    path,
                    "true",
                    String.valueOf(executorNum),
                    String.valueOf(executorMemory),
                    String.valueOf(executorCores),
                    args);
            long startTime = System.nanoTime();
            launcher.startApplication(new SparkAppHandle.Listener(){
                @Override
                public void stateChanged(SparkAppHandle handle) {
                    if (handle.getState().isFinal()) {
                        String yarnLogUrl = "";
                        String yarnLog = "";
                        try {
                            yarnLogUrl = JobUtils.getYarnLogUrl(jobConstant.rmUrl, handle.getAppId());
                            yarnLog = JobUtils.getYarnLog(yarnLogUrl);
                        } catch (IOException e) {
                            logger.warn("yarn logurl is null.");
                        }
                        logger.info("fetched logurl: {}", yarnLogUrl);
                        //TODO 暂时只是写成直接获取日志的方式，后续需要改成通过yarn的rest api获取
                        //jobDubboService.updateJobArgs(subAlgoJobId, "日志", yarnLogUrl);
                        //禁止输出长log
                        jobDubboService.updateJobArgs(subAlgoJobId, "日志", yarnLog);
                        jobDubboService.updateJobArgs(subAlgoJobId, "LO耗时", (System.nanoTime() - startTime) * 0.000000001 + "s");
                        long modelsTransferStartTime = System.nanoTime();
                        Object models = IntegrationUtils.getModels(path, algoType, algoName);
                        integrationDubboService.recordModels(subAlgoJobId, models);
                        jobDubboService.updateJobArgs(subAlgoJobId, "传输模型耗时", (System.nanoTime() - modelsTransferStartTime) * 0.000000001 + "s");
                        jobDubboService.endSubJob(subAlgoJobId, handle.getState().toString());
                        integrationDubboService.checkAndIntegrate(subAlgoJobId, algoType, algoName, args);
                        return;
                    } else if(Objects.equals(handle.getState().toString(), "SUBMITTED")){
                        String sparkJobId = handle.getAppId();
                        if(sparkJobId != null && !sparkJobId.isEmpty()){
                            jobDubboService.updateJobArgs(subAlgoJobId ,"Spark任务ID", sparkJobId);
                        }
                    }
                    jobDubboService.updateJobStatus(subAlgoJobId, handle.getState().toString());
                    jobDubboService.syncInDB(subAlgoJobId);
                    logger.info("Spark Job State: {}", handle.getState().toString());
                }
                @Override
                public void infoChanged(SparkAppHandle handle) {
                    logger.info("Spark Job Info: {}", handle.getState().toString());
                }
            });
        } catch (IOException e) {
            jobDubboService.endSubJob(subAlgoJobId, "FAILED");
            throw new RuntimeException(e);
        }
    }

    @Async("taskExecutor")
    @Override
    public void toAlgoLogo(
            String userId,
            String algoType,
            String algoSubSetting,
            String algoName,
            String filename,
            int executorNum,
            int executorMemory,
            int executorCores,
            String modelType,
            String model,
            String... args) {
        String HDFS_IP = "hdfs://172.31.238.102:8020";
        logger.info("正在发起调用算法执行调用");
        JobLogoInfo jobInfo = new JobLogoInfo(userId, 3, algoName, "Waiting");
        logger.info("algoType:" + algoType +  " algoName:" + algoName, " filename:" + filename);
        UserInfo user = userDubboService.queryUserInfo(userId);
        jobInfo.setExecutorArgs(executorNum,executorCores,executorMemory);
        logger.info("+++++++++++++++++++"+userId+"++++++++++++++++++");
        int jobId = jobDubboLogoService.createJob(jobInfo);
        //更新用户正在使用的资源
        logger.info("准备开始任务，正在更新用户资源");
        UserResource userResource = userResourceDubboService.queryUserResourceByUserId(userId);
        logger.info("+++++++++++++++++++"+userResource.toString()+"++++++++++++++++++");
        int useCpu = userResource.getUseCpu() + executorNum * executorCores;
        userResource.setUseCpu(useCpu);
        long useMem = userResource.getUseMemory() + executorMemory * 1024L * executorNum;
        userResource.setUseMemory(useMem);
        userResourceDubboService.updateUserResource(userResource);
        logger.info("+++++++++++++++++++"+userResource.toString()+"++++++++++++++++++");
        try {
            String path = constant.url + constant.modelPrefix + UUID.randomUUID();
            SparkAppHandle handler = getCorrespondingLauncher(algoType, algoSubSetting, algoName, filename, path, "false",
                    String.valueOf(executorNum), String.valueOf(executorMemory), String.valueOf(executorCores), " ")
                    .startApplication(new SparkAppHandle.Listener(){
                        @Override
                        public void stateChanged(SparkAppHandle handle) {
                            if(handle.getState().isFinal()){
                                //结束的时候更新
                                logger.info("准备结束，正在更新数据");
                                userResource.setUseMemory(userResource.getUseMemory() - executorMemory * 1024L * executorNum);
                                userResource.setUseCpu(userResource.getUseCpu() - executorNum * executorCores);
//                                userResource.update(userResource);
                                userResourceDubboService.updateUserResource(userResource);
                                String yarnLogUrl = "";
                                String yarnLog = "";
                                try {
                                    yarnLogUrl = JobUtils.getYarnLogUrl(jobConstant.rmUrl, handle.getAppId());
                                    yarnLog = JobUtils.getYarnLog(yarnLogUrl);
                                } catch (IOException e) {
                                    logger.warn("yarn logurl is null.");
                                }
                                logger.info("fetched logurl: {}", yarnLogUrl);
                                //结束时间
                                //根据生成模型日志的类型在hdfs建立相应的模型区,modelType为模型类型，model为模型,yarnLog为模型数据
                                HDFS_operation hdfsOperation = new HDFS_operation();
                                hdfsOperation.setNameNodeIP(HDFS_IP);
                                //设置用户
                                hdfsOperation.setHdfsUserName(user.getUserName());
                                String rootPath = "/user/" + hdfsOperation.getHdfsUserName() + "/model" + "/" + modelType;
                                //创建模型类别文件夹
                                hdfsOperation.mkdir(rootPath);
                                String detailPath = rootPath + "/" + model + "-0" + ".txt";
                                logger.info("将要保存的文件名为：" + detailPath);
                                //将模型文件写入模型类别文件夹
                                try {
                                    boolean file = hdfsOperation.createFile(detailPath, yarnLog);
                                } catch (URISyntaxException | InterruptedException | IOException e) {
                                    throw new RuntimeException(e);
                                }


                            }
                            else if (Objects.equals(handle.getState().toString(), "SUBMITTED")) {
                                String sparkJobId = handle.getAppId();
                                if(sparkJobId != null && !sparkJobId.isEmpty()){
                                    jobDubboLogoService.updateJobArgs(jobId ,"Spark任务ID", sparkJobId);
                                }
                            }
                            //没考虑中间中断时，资源的变化
                            jobDubboLogoService.updateJobStatus(jobId,handle.getState().toString());
                            jobDubboLogoService.syncInDB(jobId);
                        }
                        @Override
                        public void infoChanged(SparkAppHandle handle) {
                            logger.info("Spark Job Info: {}", handle.getState().toString());

                        }
                    });
            System.out.println(handler.getState());
        } catch (IOException e) {
//                job执行失败,归还资源
            logger.info("任务执行失败");
            userResource.setUseMemory(userResource.getUseMemory() - executorMemory * 1024L * executorNum);
            userResource.setUseCpu(userResource.getUseCpu() - executorNum * executorCores);
            userResourceDubboService.updateUserResource(userResource);
            jobDubboLogoService.endJob(jobId,"FAILED");
            throw new RuntimeException(e);
        }

        logger.info("等待算法调用结束");
    }

    protected SparkLauncher getCorrespondingLauncher(
            String algoType,
            String algoSubSetting,
            String algoName,
            String filename,
            String modelPath,
            String isCrossDomain,
            String executorNum,
            String executorMemory,
            String executorCores,
            String args){
        SparkLauncher sparkLauncher = new SparkLauncher()
                .setAppName(
                        "Octopus-LOGO-" +
                                algoType + " - " +
                                algoName + " - " +
                                algoSubSetting + " - source:" +
                                filename + " - models:" +
                                modelPath + " - executorNum:" +
                                executorNum + " - executorMemory:" +
                                executorMemory + " - executorCores:" +
                                executorCores + " - args:" + String.join(" ", args))
                .setMaster("yarn")
                .setDeployMode("cluster")
                .setConf("spark.eventLog.enabled", "true")
                .setConf("spark.dynamicAllocation.minExecutors", "1")
                .setConf("spark.dynamicAllocation.enabled", "true")
                .setConf("spark.shuffle.service.enabled", "true")
                .setConf("spark.metrics.conf", "/opt/cloudera/parcels/CDH/lib/spark/conf/metrics.properties")
                //.setConf("spark.metrics.conf", "hdfs:///172.31.238.102:8080/user/zhaolingxiang/metrics.properties")
//                .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH,constant.url + "/user/zhaolingxiang/rspmanager/algo/spark-rsp_2.11-2.4.0.jar")
//                .setConf(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH,constant.url + "/user/zhaolingxiang/rspmanager/algo/spark-rsp_2.11-2.4.0.jar")
//                .addJar(constant.url + "/user/zhaolingxiang/rspmanager/algo/spark-rsp_2.11-2.4.0.jar")
                //.setAppResource(constant.url + constant.algoPrefix + "rsp-algos-1.0-SNAPSHOT-jar-with-dependencies.jar")
                .setAppResource(constant.url + constant.algoPrefix + "spark-octopus-algo_2.11-2.4.0-jar-with-dependencies.jar")
                //.setMainClass("com.szubd.rspalgos.App")
                .setMainClass("org.apache.spark.logo.App")
                .addAppArgs(
                        algoType,
                        algoSubSetting,
                        algoName,
                        filename,
                        modelPath,
                        isCrossDomain);
        if(StringUtils.isNotBlank(args)){
            for (String arg : args.split(" ")) {
                sparkLauncher.addAppArgs(arg);
            }
        }
        if(!"-1".equals(executorNum)){
            sparkLauncher.setConf("spark.dynamicAllocation.maxExecutors",  executorNum);
        }
        if(!"-1".equals(executorMemory)){
            sparkLauncher.setConf("spark.executor.memory",  executorMemory + "g");
        }
        if(!"-1".equals(executorCores)){
            sparkLauncher.setConf("spark.executor.cores",  executorCores);
        }
//        switch (algoType) {
//            //0.05 1 0.05 1 50 405 405 405
//            case "clf" :
//                //sparkLauncher.addAppArgs(algoName, "/user/caimeng/classification_100_2_0.82_24000_3T.parquet","-0.9", "8", "10", "20");
//                sparkLauncher.addAppArgs(algoName, filename, modelPath, "0.8", "0", "0.05", "1", "10", "10", "10");
//                break;
//            case "clt" :
//                //sparkLauncher.addAppArgs(algoName, "/user/caimeng/classification_100_2_0.82_24000_3T.parquet"," Centers.parquet:rspclf100_8000.parquet", "0.05", "0", "8", "10","20");
//                sparkLauncher.addAppArgs(algoName, filename, "/user/zhaolingxiang/rspmanager/algo/CenterClf.parquet:rspclf100_8000.parquet", modelPath, "0.8", "0", "1", "10");
//                break;
//            case "fpg" :
//                //sparkLauncher.addAppArgs("/user/caimeng/Item_50_5_txtToRSP", "0.15", "0.5", "25", "50");
//                sparkLauncher.addAppArgs(filename, modelPath, "0.15", "0.5", "2", "5");
//                break;
//            default :
//                throw new RuntimeException("算法类型错误");
//        }
        return sparkLauncher;
    }
}
