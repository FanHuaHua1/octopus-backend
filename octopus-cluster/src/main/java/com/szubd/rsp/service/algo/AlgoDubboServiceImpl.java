package com.szubd.rsp.service.algo;

import com.szubd.rsp.algo.AlgoDubboService;
import com.szubd.rsp.constants.JobConstant;
import com.szubd.rsp.constants.RSPConstant;
import com.szubd.rsp.integration.IntegrationDubboService;
import com.szubd.rsp.job.JobDubboService;
import com.szubd.rsp.job.JobInfo;
import com.szubd.rsp.tools.JobUtils;
import com.szubd.rsp.tools.IntegrationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.dubbo.config.annotation.DubboReference;
import org.apache.dubbo.config.annotation.DubboService;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

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
    private IntegrationDubboService integrationDubboService;
    @DubboReference
    private JobDubboService jobDubboService;
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
            JobInfo subAlgoJobInfo = new JobInfo(1, "algo", "RUNNING" , constant.ip, jobId);
            int subAlgoJobId = jobDubboService.createJob(subAlgoJobInfo);
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
                                String yarnLog = null;
                                try {
                                    yarnLog = JobUtils.getYarnLog(jobConstant.rmUrl, handle.getAppId());
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                                long loTimeConsuming = System.nanoTime() - startTime;
                                long modelsTransferStartTime = System.nanoTime();
                                Object models = IntegrationUtils.getModels(path, algoType, algoName);
                                //integrationDubboService.recordModels(subAlgoJobId, models);
                                integrationDubboService.recordModelPaths(subAlgoJobId, path);
                                //禁止输出长log
                                jobDubboService.updateMultiJobArgs(subAlgoJobId,
                                        "数据集", filename,
                                        "日志", yarnLog,
                                        "ip", constant.ip,
                                        "loTimeConsuming", String.format("%.2f",loTimeConsuming * 0.000000001),
                                        "modelsTransferTimeConsuming", String.format("%.2f",(System.nanoTime() - modelsTransferStartTime) * 0.000000001)
                                );
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
                       String algoType,
                       String algoSubSetting,
                       String algoName,
                       String filename,
                       int executorNum,
                       int executorMemory,
                       int executorCores,
                       String... args) {
            logger.info("正在发起调用算法执行调用");
            try {
                String path = constant.url + constant.modelPrefix + UUID.randomUUID();
                SparkAppHandle handler = getCorrespondingLauncher(algoType, algoSubSetting, algoName, filename, path, "false", "-1", "-1", "-1", " ")
                    .startApplication(new SparkAppHandle.Listener(){
                        @Override
                        public void stateChanged(SparkAppHandle handle) {
                        }
                        @Override
                        public void infoChanged(SparkAppHandle handle) {
                            logger.info("Spark Job Info: {}", handle.getState().toString());
                        }
                    });
            } catch (IOException e) {
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
                //.setDeployMode("client")
                .setConf("spark.eventLog.enabled", "true")
                .setConf("spark.dynamicAllocation.minExecutors", "1")
                .setConf("spark.dynamicAllocation.enabled", "true")
                .setConf("spark.shuffle.service.enabled", "true")
                .setConf("spark.metrics.conf", "/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/etc/spark/metrics.properties")
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
//        if(!"-1".equals(executorNum)){
//            sparkLauncher.setConf("spark.dynamicAllocation.maxExecutors",  executorNum);
//        }
//        if(!"-1".equals(executorMemory)){
//            sparkLauncher.setConf("spark.executor.memory",  executorMemory + "g");
//        }
//        if(!"-1".equals(executorCores)){
//            sparkLauncher.setConf("spark.executor.cores",  executorCores);
//        }
        sparkLauncher.setConf("spark.executor.memory",  "12g");
        sparkLauncher.setConf("spark.executor.cores",  "3");
        //sparkLauncher.setConf("spark.executor.memoryOverhead",  "250");

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
