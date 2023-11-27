package com.szubd.rsp.service.operation;
import com.szubd.rsp.algo.OperationDubboService;
import com.szubd.rsp.job.JobDubboService;
import com.szubd.rsp.WebSocketConnectService;
import com.szubd.rsp.constants.RSPConstant;
import com.szubd.rsp.job.JobInfo;
import org.apache.dubbo.config.annotation.DubboReference;
import org.apache.dubbo.config.annotation.DubboService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.util.StringUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.apache.hadoop.tools.DistCpOptions;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Component
@DubboService(async = true)
public class RSPDubboService implements OperationDubboService {
    @Autowired
    private RSPConstant constant;
    @DubboReference
    private WebSocketConnectService webSocketConnectService;
    @DubboReference
    private JobDubboService jobDubboService;

    @Async("taskExecutor")
    public void toRsp(int jobId, String originName, String targetName, String blockNum, String type) throws Exception{
        new SparkLauncher()
            .setAppName("GeneralRSP")
            .setMaster("yarn")
//            .setConf("spark.driver.memory", "4g")
//            .setConf("spark.executor.memory", "4g")
//            .setConf("spark.executor.cores", "2")
            .setConf("spark.eventLog.enabled", "true")
            .setConf("spark.eventLog.dir", constant.url + "/user/spark/applicationHistory")
            .setAppResource(constant.url + "/user/zhaolingxiang/rspmanager/app/octopus-core-1.0-SNAPSHOT-jar-with-dependencies.jar")
//            .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH,constant.url + "/user/zhaolingxiang/rspmanager/algo/spark-rsp_2.11-2.4.0.jar")
//            .setConf(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH,constant.url + "/user/zhaolingxiang/rspmanager/algo/spark-rsp_2.11-2.4.0.jar")
//            .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH,"./spark-rsp_2.11-2.4.0.jar")
//            .setConf(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH,"./spark-rsp_2.11-2.4.0.jar")
            .setMainClass("com.szubd.rsp.ToRsp")
            .addAppArgs(constant.originPrefix + originName, constant.localRspPrefix + targetName, blockNum, type)
            .setDeployMode("cluster")
            .startApplication(new SparkAppHandle.Listener(){
                @Override
                public void stateChanged(SparkAppHandle handle) {
                    if (handle.getState().isFinal()) {
                       jobDubboService.endJob(jobId, handle.getState().toString());
                       if(handle.getState() == SparkAppHandle.State.FINISHED){
                           try {
                               constant.getSuperFileSystem().setOwner(new Path(constant.localRspPrefix + targetName), "zhaolingxiang", "zhaolingxiang");
                           } catch (URISyntaxException e) {
                               throw new RuntimeException(e);
                           } catch (IOException e) {
                               throw new RuntimeException(e);
                           } catch (InterruptedException e) {
                               throw new RuntimeException(e);
                           }
                       }
                       return;
                    } else if(Objects.equals(handle.getState().toString(), "SUBMITTED")){
                        String sparkJobId = handle.getAppId();
                        if(sparkJobId != null && !sparkJobId.equals("")){
                            jobDubboService.updateJobArgs(jobId ,"Spark任务ID", sparkJobId);
                        }
                    }
                    jobDubboService.updateJobStatus(jobId, handle.getState().toString());
                    jobDubboService.syncInDB(jobId);
                    System.out.println("state:" + handle.getState().toString());
                    //webSocketConnectService.sendToRspMsg(handle.getState().toString());
                }

                @Override
                public void infoChanged(SparkAppHandle handle) {}
            });
    }

    @Async("taskExecutor")
    public void mergeRSP(String tmpPath, String fileList, int repartitionNum, String mixType) throws Exception{
        CountDownLatch countDownLatch = new CountDownLatch(1);
        SparkAppHandle handler = new SparkLauncher()
            .setAppName("MixRSP-Merge")
            .setMaster("yarn")
//            .setConf("spark.driver.memory", "4g")
//            .setConf("spark.executor.memory", "4g")
//            .setConf("spark.executor.cores", "2")
            .setConf("spark.eventLog.enabled", "true")
            //.setConf("spark.eventLog.dir", "hdfs://nameservice1/user/spark/applicationHistory")
                //TODO:前缀改成配置文件
            .setAppResource(constant.url + "/user/zhaolingxiang/rspmanager/app/octopus-core-1.0-SNAPSHOT-jar-with-dependencies.jar")
            .setMainClass("com.szubd.rsp.MergeRSP")
            .addAppArgs(tmpPath, fileList, repartitionNum + "" ,mixType)
            .setDeployMode("cluster")
            .startApplication(new SparkAppHandle.Listener(){
                @Override
                public void stateChanged(SparkAppHandle handle) {
                    if (handle.getState().isFinal()) {
                        countDownLatch.countDown();
                        if(handle.getState() == SparkAppHandle.State.FINISHED){

                        }
                    }
                    System.out.println("state:" + handle.getState().toString());
                    webSocketConnectService.sendToRspMsg(handle.getState().toString());
                }

                @Override
                public void infoChanged(SparkAppHandle handle) {
                    System.out.println("Info:" + handle.getState().toString());
                }
            });
            System.out.println("The task is executing, please wait ....");
            countDownLatch.await();
    }


//    @Override
//    public void toRspMix(List<String> fileList, String father) {
//        String dst = constant.url + constant.globalRspPrefix + father.split("-")[0] + "/" + father;
//        distcp(fileList, dst);
//    }

    @Async("taskExecutor")
    @Override
    public void toRspMix(List<String>[] fileList, String father, int jobId, int repartitionNum, String mixType) throws Exception {
        String dst = constant.url + constant.logPrefix + father;
        String savePath = constant.url + constant.globalRspPrefix + father.split("-")[0] + "/" + father;
        Stream<String> stringStream = Arrays.stream(fileList).flatMap(List::stream);
        //Stream<String> stringStream = fileList.stream().flatMap(List::stream);
        List<String> totalList = stringStream.collect(Collectors.toList());
        jobDubboService.updateJobArgs(jobId, "目的目录", dst);
        jobDubboService.updateJobStatus(jobId, "RUNNING");
        jobDubboService.syncInDB(jobId);
        distcp(totalList, dst);
        jobDubboService.endJob(jobId, "FINISHED");
        String host;
        try {
            host = new URI(constant.url).getHost();
        } catch (URISyntaxException e) {
            host = "wrong";
        }
        JobInfo mergeJobInfo = new JobInfo(1, "merge", "RUNNING", host, jobDubboService.getParentId(jobId));
        int mergeJobId = jobDubboService.createJob(mergeJobInfo);
        //List<String> collect = fileList.stream().map(
        List<String> collect = Arrays.stream(fileList).map(
                list -> {
                    List<String> collect1 = list.stream().map(
                            s -> s.lastIndexOf("/") == -1 ? s : s.substring(s.lastIndexOf("/") + 1)
                    ).collect(Collectors.toList());
                    String join = StringUtils.join(",", collect1);
                    return join;
                }
        ).collect(Collectors.toList());
        String args = StringUtils.join(":", collect);
        jobDubboService.updateJobArgs(mergeJobId, "文件临时目录", dst);
        //jobDubboService.updateJobArgs(mergeJobId, "合并的文件列表", args);
        jobDubboService.syncInDB(mergeJobId);
        try {
                mergeRSP(dst, args, repartitionNum, mixType);
                jobDubboService.endJob(mergeJobId, "FINISHED");
            } catch (Exception e) {
                jobDubboService.endJob(mergeJobId, "FAILED");
                throw new RuntimeException(e);
            }
            String hostx;
            try {
                hostx = new URI(constant.url).getHost();
            } catch (URISyntaxException e) {
                hostx = "wrong";
            }
            JobInfo mvJobInfo = new JobInfo(1, "moveFile", "RUNNNIG", hostx, jobDubboService.getParentId(jobId));
            mvJobInfo.addArgs("源路径", dst);
            mvJobInfo.addArgs("目的路径", savePath);
            int mvJobId = jobDubboService.createJob(mvJobInfo);
            try {
                mvTmpFile(dst, savePath);
                jobDubboService.updateJobStatus(mvJobId, "FINISHED");
            } catch (URISyntaxException e) {
                jobDubboService.updateJobStatus(mvJobId, "FAILED");
                throw new RuntimeException(e);
            } catch (IOException e) {
                jobDubboService.updateJobStatus(mvJobId, "FAILED");
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                jobDubboService.updateJobStatus(mvJobId, "FAILED");
                throw new RuntimeException(e);
            }
            jobDubboService.endSubJob(mvJobId, "FINISHED");
    }

    @Async("taskExecutor")
    private int distcp(List<String> srcPaths, String dst) {
        try {
            Stream<Path> pathStream = srcPaths.stream().map(string -> new Path(string));
            List<Path> srcPathsList = pathStream.collect(Collectors.toList());
            DistCpOptions options = new DistCpOptions.Builder(srcPathsList, new Path(dst))
           .withOverwrite(true)
           .maxMaps(100)
           .preserve(DistCpOptions.FileAttribute.BLOCKSIZE)
           //.withLogPath(new Path(url + "/user/zhaolingxiang/rspmanager/log"))
           .build();
            Configuration configuration = new Configuration();

            DistCp distcp = new DistCp(configuration, options);
//            int run = ToolRunner.run(configuration, distcp, constructDistCpParams(srcPathsList, new Path(dst)));
            Job execute = distcp.execute();
            System.out.println(execute.getHistoryUrl());
            execute.waitForCompletion(true);
            return 1;
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return -1;
    }


//    private int distcp2(List<List<String>> groupPaths, List<String> srcPaths, String dst) {
//        try {
//            Stream<Path> pathStream = srcPaths.stream().map(string -> new Path(string));
//            List<Path> srcPathsList = pathStream.collect(Collectors.toList());
//            DistCpOptions options = new DistCpOptions.Builder(srcPathsList, new Path(dst))
//           .withOverwrite(true)
//           .maxMaps(100)
//           .preserve(DistCpOptions.FileAttribute.BLOCKSIZE)
//           .build();
//            Configuration configuration = new Configuration();
//            DistCp distcp = new DistCp(configuration, options);
//            Job execute = distcp.execute();
//            System.out.println(execute.getHistoryUrl());
//            execute.waitForCompletion(true);
//            return 1;
//        } catch (Exception e) {
//            System.out.println(e.getMessage());
//        }
//        return -1;
//    }
//    private static String[] constructDistCpParams(List<Path> srcPaths, Path dst) {
//        List<String> params = new ArrayList<>();
//        for (Path src : srcPaths) {
//          params.add(src.toString());
//        }
//        params.add(dst.toString());
//        return params.toArray(new String[params.size()]);
//    }

    public void mvTmpFile(String tempPath, String savePath) throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = constant.getSuperFileSystem();
        Path path = new Path(savePath);
        fileSystem.mkdirs(path);
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(tempPath));
        for (FileStatus fileStatus : fileStatuses) {
            if(!fileStatus.isDirectory()) continue;
            FileStatus[] blockStatuses = fileSystem.listStatus(fileStatus.getPath());
            for (FileStatus blockStatus : blockStatuses) {
                fileSystem.rename(blockStatus.getPath(), new Path(savePath));
            }
        }
        fileSystem.delete(new Path(tempPath));
        fileSystem.setOwner(path, "zhaolingxiang", "zhaolingxiang");
    }
}
