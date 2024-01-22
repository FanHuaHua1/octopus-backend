package com.szubd.rsp.service.operation;
import com.szubd.rsp.algo.OperationDubboService;
import com.szubd.rsp.job.JobDubboService;
import com.szubd.rsp.WebSocketConnectService;
import com.szubd.rsp.constants.RSPConstant;
import com.szubd.rsp.job.JobInfo;
import org.apache.commons.io.FileUtils;
import org.apache.dubbo.config.annotation.DubboReference;
import org.apache.dubbo.config.annotation.DubboService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpContext;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.SparkFiles;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.apache.hadoop.tools.DistCpOptions;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
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
            .setAppResource(constant.url + constant.app + "octopus-core-1.0-SNAPSHOT-jar-with-dependencies.jar")
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
                               constant.getSuperFileSystem().setOwner(new Path(constant.localRspPrefix + targetName), constant.user, constant.user);
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
    public void mergeRSP(String tmpPath, String savePath, int repartitionNum, String mixType) throws Exception{
        CountDownLatch countDownLatch = new CountDownLatch(1);
//        String fileListTmpUrl = tmpPath + "-mergeRspTmpFile.txt";
//        changeFileOwner(tmpPath);
//        writeStringToHDFS(fileListTmpUrl, fileList);
        System.out.println(tmpPath);
        System.out.println(savePath);
        HashMap env = new HashMap();
        //这两个属性必须设置
        env.put("HADOOP_CONF_DIR","/etc/hadoop/conf");
        SparkAppHandle handler = new SparkLauncher(env)
            .setAppName("MixRSP-Merge")
            .setMaster("yarn")
//            .setConf("spark.driver.memory", "4g")
            .setConf("spark.executor.memory", "3g")
//            .setConf("spark.executor.cores", "2")
            .setConf("spark.eventLog.enabled", "true")
            //.setConf("spark.eventLog.dir", "hdfs://nameservice1/user/spark/applicationHistory")
            .setAppResource(constant.url + constant.app + "octopus-core-1.0-SNAPSHOT-jar-with-dependencies.jar")
            .setMainClass("com.szubd.rsp.MergeRSP3")
            .addAppArgs(tmpPath, savePath, repartitionNum + "" ,mixType)
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


    protected void writeStringToHDFS(String filename, String content){
        try {
            FileSystem fs = constant.getFileSystem();
            FSDataOutputStream outputStream = fs.create(new Path(filename));
            outputStream.write(content.getBytes(StandardCharsets.UTF_8));
            outputStream.close();
            System.out.println("File written to HDFS successfully.");
        } catch (Exception e) {
            System.out.println(e);
        }
    }
    protected void changeFileOwner(String filename){
        try {
            FileSystem fs = constant.getSuperFileSystem();
            fs.setOwner(new Path(filename), constant.user, constant.user);
            System.out.println("File owner changed successfully.");
        } catch (Exception e) {
            System.out.println(e);
        }
    }
//    @Override
//    public void toRspMix(List<String> fileList, String father) {
//        String dst = constant.url + constant.globalRspPrefix + father.split("-")[0] + "/" + father;
//        distcp(fileList, dst);
//    }

//    @Async("taskExecutor")
//    @Override
//    public void toRspMix(List<String>[] fileList, String father, int jobId, int repartitionNum, String mixType) throws Exception {
//        String dst = constant.url + constant.logPrefix + father;
//        String savePath = constant.url + constant.globalRspPrefix + father.split("-")[0] + "/" + father;
//        Stream<String> stringStream = Arrays.stream(fileList).flatMap(List::stream);
//        //Stream<String> stringStream = fileList.stream().flatMap(List::stream);
//        List<String> totalList = stringStream.collect(Collectors.toList());
//        jobDubboService.updateJobArgs(jobId, "目的目录", dst);
//        jobDubboService.updateJobStatus(jobId, "RUNNING");
//        jobDubboService.syncInDB(jobId);
//        distcp(totalList, dst);
//        jobDubboService.endJob(jobId, "FINISHED");
//        String host;
//        try {
//            host = new URI(constant.url).getHost();
//        } catch (URISyntaxException e) {
//            host = "wrong";
//        }
//        JobInfo mergeJobInfo = new JobInfo(1, "merge", "RUNNING", host, jobDubboService.getParentId(jobId));
//        int mergeJobId = jobDubboService.createJob(mergeJobInfo);
//        //List<String> collect = fileList.stream().map(
//        List<String> collect = Arrays.stream(fileList).map(
//                list -> {
//                    List<String> collect1 = list.stream().map(
//                            s -> s.lastIndexOf("/") == -1 ? s : s.substring(s.lastIndexOf("/") + 1)
//                    ).collect(Collectors.toList());
//                    String join = StringUtils.join(",", collect1);
//                    return join;
//                }
//        ).collect(Collectors.toList());
//        String args = StringUtils.join(":", collect);
//        jobDubboService.updateJobArgs(mergeJobId, "文件临时目录", dst);
//        //jobDubboService.updateJobArgs(mergeJobId, "合并的文件列表", args);
//        jobDubboService.syncInDB(mergeJobId);
//        try {
//                mergeRSP(dst, args, repartitionNum, mixType);
//                jobDubboService.endJob(mergeJobId, "FINISHED");
//            } catch (Exception e) {
//                jobDubboService.endJob(mergeJobId, "FAILED");
//                throw new RuntimeException(e);
//            }
//            String hostx;
//            try {
//                hostx = new URI(constant.url).getHost();
//            } catch (URISyntaxException e) {
//                hostx = "wrong";
//            }
//            JobInfo mvJobInfo = new JobInfo(1, "moveFile", "RUNNNIG", hostx, jobDubboService.getParentId(jobId));
//            mvJobInfo.addArgs("源路径", dst);
//            mvJobInfo.addArgs("目的路径", savePath);
//            int mvJobId = jobDubboService.createJob(mvJobInfo);
//            try {
//                mvTmpFile(dst, savePath);
//                jobDubboService.updateJobStatus(mvJobId, "FINISHED");
//            } catch (URISyntaxException e) {
//                jobDubboService.updateJobStatus(mvJobId, "FAILED");
//                throw new RuntimeException(e);
//            } catch (IOException e) {
//                jobDubboService.updateJobStatus(mvJobId, "FAILED");
//                throw new RuntimeException(e);
//            } catch (InterruptedException e) {
//                jobDubboService.updateJobStatus(mvJobId, "FAILED");
//                throw new RuntimeException(e);
//            }
//            jobDubboService.endSubJob(mvJobId, "FINISHED");
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

//    @Async("taskExecutor")
//    @Override
//    public void distcpBeforeMix(List<String> fileList, String father, int jobId, int fatherJobId) {
//        String dst = constant.url + constant.logPrefix + father;
//        String savePath = constant.url + constant.globalRspPrefix + father.split("-")[0] + "/" + father;
//        jobDubboService.updateJobArgs(jobId, "目的目录", dst);
//        jobDubboService.updateJobStatus(jobId, "RUNNING");
//        jobDubboService.syncInDB(jobId);
//        System.out.println("传输开始");
//        long l = System.currentTimeMillis();
//        distcp(fileList, dst);
//        System.out.println("传输耗时" + (System.currentTimeMillis() - l) / 1000 + "s");
//        jobDubboService.reduceJobCountDown(fatherJobId);
//        jobDubboService.endJob(jobId, "FINISHED");
//    }

    @Async("taskExecutor")
    @Override
    public void distcpBeforeMix(List<String> fileList, String[] taskList, String father, int jobId, int fatherJobId) throws InterruptedException {
        String dst = constant.url + constant.logPrefix + father;
        Map<String, List<String>> nodeFileListMap = new HashMap<>();
        for (String file : fileList) {
            String inetAddr = null;
            try {
                inetAddr = InetAddress.getByName(new URI(file).getHost()).getHostAddress();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            List<String> fl = nodeFileListMap.get(inetAddr);
                if(fl == null){
                    fl = new LinkedList<>();
                    nodeFileListMap.put(inetAddr, fl);
                }
                fl.add(file);
        }
        String savePath = constant.url + constant.globalRspPrefix + father.split("-")[0] + "/" + father;
        jobDubboService.updateJobArgs(jobId, "目的目录", dst);
        jobDubboService.updateJobStatus(jobId, "RUNNING");
        jobDubboService.syncInDB(jobId);
        System.out.println("传输开始");
        long l = System.nanoTime();
        CountDownLatch countDownLatch = new CountDownLatch(nodeFileListMap.size());
        CountDownLatch localCountDownLatch = new CountDownLatch(taskList.length);
        int crossDomainTaskSum = taskList.length;
        for(int i = 0; i < crossDomainTaskSum; i++){
            int leftCrossDomainTaskToSubmit = crossDomainTaskSum - i;
            List<String> taskListSrc = nodeFileListMap.get(taskList[i]);
            String nodeDst = dst + "/" + taskList[i];
            new Thread(() -> distcpWithCount(taskListSrc, nodeDst, countDownLatch, localCountDownLatch, false)).start();
//            while(localCountDownLatch.getCount() == leftCrossDomainTaskToSubmit){
//                Thread.sleep(1);
//            }
        }
        localCountDownLatch.await();
        new Thread(() -> distcpWithCount(nodeFileListMap.get(constant.ip), dst + "/" + constant.ip,countDownLatch, null, true)).start();
//        for (Map.Entry<String, List<String>> stringListEntry : nodeFileListMap.entrySet()) {
//            String nodeDst = dst + "/" + stringListEntry.getKey();
//            distcp(stringListEntry.getValue(), nodeDst);
//        }
        countDownLatch.await();
        double transferDuration =  (System.nanoTime() - l) * 0.000000001;
        System.out.println("传输耗时" + transferDuration + "s");
        jobDubboService.updateJobArgs(jobId, "transfer-duration", transferDuration + "s");
        jobDubboService.syncInDB(jobId);
        jobDubboService.reduceJobCountDown(fatherJobId);
        jobDubboService.endJob(jobId, "FINISHED");
    }

    public int distcp(List<String> srcPaths, String dst) {
        try {
            Stream<Path> pathStream = srcPaths.stream().map(string -> new Path(string));
            List<Path> srcPathsList = pathStream.collect(Collectors.toList());
/*            DistCpOptions options = new DistCpOptions.Builder(srcPathsList, new Path(dst))
                    .withOverwrite(true)
                    .preserve(DistCpOptions.FileAttribute.BLOCKSIZE)
                    .preserve(DistCpOptions.FileAttribute.GROUP)
                    .preserve(DistCpOptions.FileAttribute.USER)
                    .preserve(DistCpOptions.FileAttribute.REPLICATION)
                    .withCopyStrategy("dynamic")
                    .maxMaps(50)
                    // .withLogPath(new Path(url + "/user/zhaolingxiang/rspmanager/log"))
                    .build();*/

            DistCpOptions.Builder builder = new DistCpOptions.Builder(srcPathsList, new Path(dst))
                    .withOverwrite(true)
                    .preserve(DistCpOptions.FileAttribute.BLOCKSIZE)
                    .preserve(DistCpOptions.FileAttribute.GROUP)
                    .preserve(DistCpOptions.FileAttribute.USER)
                    .preserve(DistCpOptions.FileAttribute.REPLICATION)
                    .withCopyStrategy("dynamic")
                    .withBlocking(true);
            if(dst.startsWith("hdfs://192.168.0.67")){
                builder = builder.maxMaps(106);
            } else if(dst.contains("hdfs://192.168.0.23")) {
                builder = builder.maxMaps(70);
            } else {
                builder = builder.maxMaps(46);
            }
            DistCpOptions options = builder.build();

            Configuration configuration = new Configuration();
            configuration.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
            configuration.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
            configuration.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
            configuration.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));
            DistCp distcp = new DistCp(configuration, options);

            Job execute = distcp.execute();
            System.out.println(execute.getHistoryUrl());
            execute.waitForCompletion(true);
            return 1;
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return -1;
    }
    public void distcpWithCount(List<String> srcPaths, String dst, CountDownLatch cd, CountDownLatch localCD, boolean isLocal) {
        try {
            Stream<Path> pathStream = srcPaths.stream().map(string -> new Path(string));
            List<Path> srcPathsList = pathStream.collect(Collectors.toList());
/*            DistCpOptions options = new DistCpOptions.Builder(srcPathsList, new Path(dst))
                    .withOverwrite(true)
                    .preserve(DistCpOptions.FileAttribute.BLOCKSIZE)
                    .preserve(DistCpOptions.FileAttribute.GROUP)
                    .preserve(DistCpOptions.FileAttribute.USER)
                    .preserve(DistCpOptions.FileAttribute.REPLICATION)
                    .withCopyStrategy("dynamic")
                    .maxMaps(50)
                    // .withLogPath(new Path(url + "/user/zhaolingxiang/rspmanager/log"))
                    .build();*/

            DistCpOptions.Builder builder = new DistCpOptions.Builder(srcPathsList, new Path(dst))
                    .withOverwrite(true)
                    .preserve(DistCpOptions.FileAttribute.BLOCKSIZE)
                    .preserve(DistCpOptions.FileAttribute.GROUP)
                    .preserve(DistCpOptions.FileAttribute.USER)
                    .preserve(DistCpOptions.FileAttribute.REPLICATION)
                    .withCopyStrategy("dynamic")
                    .withBlocking(false);
            if(dst.startsWith("hdfs://192.168.0.67")){
                builder = builder.maxMaps(106);
            } else if(dst.contains("hdfs://192.168.0.23")) {
                builder = builder.maxMaps(70);
            } else {
                builder = builder.maxMaps(46);
            }
            //builder = builder.maxMaps(50);
            DistCpOptions options = builder.build();
            Configuration configuration = new Configuration();
            configuration.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
            configuration.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
            configuration.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
            configuration.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));
            if(isLocal){
                configuration.set(JobContext.PRIORITY, "1");
                configuration.set(JobContext.QUEUE_NAME,"root.octopus.local");
            } else {
                configuration.set(JobContext.PRIORITY, "5");
                configuration.set(JobContext.QUEUE_NAME,"root.octopus.crossdomain");
            }
            //configuration.set("dfs.replication","1");
            DistCp distcp = new DistCp(configuration, options);
            Job execute = distcp.execute();
            if(localCD != null) {
                if(localCD.getCount() == 1){
                    while(execute.getJobState() == JobStatus.State.PREP){
                        Thread.sleep(1);
                    }
                }
                localCD.countDown();
            }
            System.out.println(execute.getHistoryUrl());
            execute.waitForCompletion(true);
            cd.countDown();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }


    @Async("taskExecutor")
    @Override
    public void rspMerge(List<List<String>> fileList, String father, int jobId, int repartitionNum, String mixType) throws Exception {
        String dst = constant.url + constant.logPrefix + father;
        String savePath = constant.url + constant.globalRspPrefix + father.split("-")[0] + "/" + father;
        String host;
        try {
            host = new URI(constant.url).getHost();
        } catch (URISyntaxException e) {
            host = "wrong";
        }
        JobInfo mergeJobInfo = new JobInfo(1, "merge", "RUNNING", host, jobId);
        int mergeJobId = jobDubboService.createJob(mergeJobInfo);
        //List<String> collect = fileList.stream().map(
//        List<String> collect = fileList.stream().map(
//                list -> {
//                    List<String> collect1 = list.stream().map(
//                            s -> s.lastIndexOf("/") == -1 ? s : s.substring(s.lastIndexOf("/") + 1)
//                    ).collect(Collectors.toList());
//                    String join = StringUtils.join(",", collect1);
//                    return join;
//                }
//        ).collect(Collectors.toList());
//        String args = StringUtils.join(":", collect);
//        List<List<String>> collect = fileList.stream().map(
//                list -> {
//                    List<String> collect1 = list.stream().map(
//                            s -> s.lastIndexOf("/") == -1 ? s : s.substring(s.lastIndexOf("/") + 1)
//                    ).collect(Collectors.toList());
//                    return collect1;
//                }
//        ).collect(Collectors.toList());
        jobDubboService.updateJobArgs(mergeJobId, "文件临时目录", dst);
        jobDubboService.syncInDB(mergeJobId);
        long startTime = System.nanoTime();
        try {
                //mergeRSP(dst, savePath, repartitionNum, mixType);
            //TODO:只计算传输时间， 传完就删除文件

                //mergeRSP(dst, savePath, fileList.size(), mixType);
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
            JobInfo mvJobInfo = new JobInfo(1, "moveFile", "RUNNNIG", hostx, jobId);
            mvJobInfo.addArgs("源路径", dst);
            mvJobInfo.addArgs("目的路径", savePath);
            int mvJobId = jobDubboService.createJob(mvJobInfo);
        deleteTmpFile(dst);
//            try {
//                //mvTmpFile(dst, savePath);
//                deleteTmpFile(dst);
//                jobDubboService.updateJobStatus(mvJobId, "FINISHED");
//            } catch (URISyntaxException e) {
//                jobDubboService.updateJobStatus(mvJobId, "FAILED");
//                throw new RuntimeException(e);
//            } catch (IOException e) {
//                jobDubboService.updateJobStatus(mvJobId, "FAILED");
//                throw new RuntimeException(e);
//            } catch (InterruptedException e) {
//                jobDubboService.updateJobStatus(mvJobId, "FAILED");
//                throw new RuntimeException(e);
//            }
            double endTime = 1.0 * (System.nanoTime() - startTime) * 0.000000001;
            jobDubboService.updateJobArgs(mergeJobId, "merge-duration", endTime +"s");
            jobDubboService.syncInDB(mergeJobId);
            jobDubboService.reduceJobCountDown(jobId);
            jobDubboService.endSubJob(mvJobId, "FINISHED");
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
        System.out.printf("开始移动文件");
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
        fileSystem.setOwner(path, constant.user, constant.user);
    }

    public void deleteTmpFile(String tempPath) throws URISyntaxException, IOException, InterruptedException {
        System.out.printf("开始移动文件");
        FileSystem fileSystem = constant.getSuperFileSystem();
        int blockNumSum = 0;
        for (FileStatus fileStatus : fileSystem.listStatus(new Path(tempPath))) {
            blockNumSum += fileSystem.listStatus(fileStatus.getPath()).length;
        }
        System.out.println("数据块数量：" + blockNumSum);
        fileSystem.delete(new Path(tempPath));
    }

    public void setFileOwner(String path) throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = constant.getSuperFileSystem();
        fileSystem.setOwner(new Path(path), constant.user, constant.user);
    }

    public void mvFilesIntoOneFolder(List<String> fileList, int blockNum, String distcpDst) throws URISyntaxException, IOException, InterruptedException {
        System.out.printf("开始移动文件");
        FileSystem fileSystem = constant.getSuperFileSystem();
        fileSystem.mkdirs(new Path(distcpDst + "/block-" + blockNum));
        Path newPath = new Path(distcpDst + "/block-" + blockNum + "/");
        for (String p : fileList) {
            FileStatus fst = fileSystem.getFileStatus(new Path(distcpDst + "/" + p));
            fileSystem.rename(fst.getPath(), newPath);
        }
        fileSystem.setOwner(newPath, constant.user, constant.user);
    }
}
