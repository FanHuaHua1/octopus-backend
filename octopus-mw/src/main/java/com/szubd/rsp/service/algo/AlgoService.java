package com.szubd.rsp.service.algo;

import com.alibaba.nacos.api.naming.pojo.Instance;
import com.szubd.rsp.algo.AlgoDubboService;
import com.szubd.rsp.job.JobInfo;
import com.szubd.rsp.node.NodeInfoService;
import com.szubd.rsp.algo.AlgoInfo;
import com.szubd.rsp.node.NodeInfo;
import com.szubd.rsp.service.job.JobService;
import com.szubd.rsp.service.node.NacosService;
import com.szubd.rsp.tools.DubboUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Component
@Slf4j
public class AlgoService {
    protected static final Logger logger = LoggerFactory.getLogger(AlgoService.class);
    @Autowired
    AlgoMapper algoMapper;
    @Autowired
    private JobService jobService;
    @Autowired
    private NodeInfoService nodeInfoService;
    @Autowired
    private NacosService nacosService;
    public List<AlgoInfo> list() {
        List<AlgoInfo> algoInfos = algoMapper.listAllAlgoInfo();
        return algoInfos;
    }
    /**
     *
     * @param algoInfos algoInfos.setAlgo("RF");
     * @param algoType
     * @param algoSubSetting algoSubSetting = "sparkShuffle";
     * @param map
     * @throws URISyntaxException
     * @throws UnknownHostException
     */
    public void submit(AlgoController.AlgoInfos algoInfos, String algoType, String algoSubSetting, Map<String,String> map) throws URISyntaxException, UnknownHostException {
        String algoName = algoInfos.algo;
        double trainRatio = algoInfos.trainRatio;
        double testRatio = algoInfos.testRatio;
        int subJobNum = algoInfos.data.size();
        String[] executorsParamsList = algoInfos.executorsParams.split(",");
        String[] expParamsList = algoInfos.expParams.split(",");
        //String[] expParamsList = Arrays.stream(algoInfos.expParams.split(" ")).mapToInt(Integer::parseInt).toArray();
        JobInfo jobInfo = new JobInfo(3, algoName, "RUNNING");
        jobInfo.addArgs("trainRatio", String.valueOf(trainRatio));
        jobInfo.addArgs("testRatio", String.valueOf(testRatio));
        jobInfo.addArgs("algoType", algoType);
        jobInfo.addArgs("algoSubSetting", algoSubSetting);
        jobInfo.addArgs("totalLoStartTime", System.currentTimeMillis() + "");
        jobInfo.addArgs("ip", Inet4Address.getLocalHost().getHostAddress());
        map.forEach(jobInfo::addAlgoArgs);
        int jobId = jobService.createCombineJob(jobInfo,subJobNum);
        //配置文件
        String[] nodeIPList = new String[subJobNum];
        ExecutorService es = Executors.newFixedThreadPool(subJobNum);
        for (int i = 0; i < subJobNum; i++) {
            //TODO:补充校验
            int[] curExecutorsParams = Arrays.stream(executorsParamsList[i].split(" ")).mapToInt(Integer::parseInt).toArray();
            String curExpParams = expParamsList[i];
            int nodeId = algoInfos.data.get(i).getNodeId();
            NodeInfo nodeInfo = nodeInfoService.queryForNodeInfoById(nodeId);
            nodeIPList[i] = nodeInfo.getNameNodeIP();
            String path = nodeInfo.getPrefix() + "globalrsp/" + algoInfos.data.get(i).getSuperName() + "/" + algoInfos.data.get(i).getGlobalrspName();
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    AlgoDubboService algoDubboService = DubboUtils.getServiceRef(nodeInfo.getIp(), "com.szubd.rsp.algo.AlgoDubboService");
                    logger.info("发起调用，目标地址：{}， 目标服务：{}", nodeInfo.getIp(), "com.szubd.rsp.algo.AlgoDubboService");
                    try {
                        algoDubboService.toAlgo(
                                            jobId,
                                            algoType,
                                            algoSubSetting,
                                            algoName,
                                            path,
                                            curExecutorsParams[0],
                                            curExecutorsParams[1],
                                            curExecutorsParams[2],
                                            curExpParams);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            es.execute(runnable);
        }
        jobService.addJobArgs(jobId, "nodeIPList", StringUtils.join(':', nodeIPList));
        jobService.syncInDB(jobId);
    }

    /**
     * 实验用
     * @param algoInfos
     * @param algoType
     * @param algoSubSetting
     * @param map
     * @throws URISyntaxException
     * @throws UnknownHostException
     */
    public void submitExp(AlgoController.AlgoInfos algoInfos, String algoType, String algoSubSetting, Map<String,String> map) throws Exception {
        String algoName = algoInfos.algo;
        double trainRatio = algoInfos.trainRatio;
        double testRatio = algoInfos.testRatio;
        int subJobNum = algoInfos.data.size();
        String[] executorsParamsList = algoInfos.executorsParams.split(",");
        String[] expParamsList = algoInfos.expParams.split(",");
        //double[][] round = new double[3][2];
        double[][] round = new double[3][23];
//        for(int i = 1; i <= 2; i++) {
//            round[0][i - 1] = i * 1.0 * 100 * 9 / 19;
//            round[1][i - 1] = i * 1.0 * 100 * 6 / 19;
//            round[2][i - 1] = i * 1.0 * 100 * 4 / 19;
//        }
        round[0][0] = 100 * 9 / 19;
        round[1][0] = 100 * 6 / 19;
        round[2][0] = 100 * 4 / 19;
//        round[0][1] = 20000 * 1.0 * 9 / 19;
//        round[1][1] = 20000 * 1.0 * 6 / 19;
//        round[2][1] = 20000 * 1.0 * 4 / 19;
//        round[0][2] = 30000 * 1.0 * 9 / 19;
//        round[1][2] = 30000 * 1.0 *  6 / 19;
//        round[2][2] = 30000 * 1.0 *  4 / 19;
        for(int i = 1; i <= 9; i++) {
            round[0][i] = i * 1.0 * 100 * 9 / 19;
            round[1][i] = i * 1.0 * 100 * 6 / 19;
            round[2][i] = i * 1.0 * 100 * 4 / 19;
        }
        for(int i = 1; i <= 9; i++) {
            round[0][i+ 9] = i * 1.0 * 1000 * 9 / 19;
            round[1][i+ 9] = i * 1.0 * 1000 * 6 / 19;
            round[2][i+ 9] = i * 1.0 * 1000 * 4 / 19;
        }
        for(int i = 1; i <= 4; i++) {
            round[0][i+ 18] = i * 1.0 * 10000 * 9 / 19;
            round[1][i+ 18] = i * 1.0 * 10000 * 6 / 19;
            round[2][i+ 18] = i * 1.0 * 10000 * 4 / 19;
        }
//        for(int i = 1; i <= 3; i++) {
//            round[0][i - 1] = (i + 1) * 1.0 * 10000 * 9 / 19;
//            round[1][i - 1] = (i + 1) * 1.0 * 10000 * 6 / 19;
//            round[2][i - 1] = (i + 1) * 1.0 * 10000 * 4 / 19;
//        }
//        String[] roundArr = Arrays.stream(round).mapToObj(i -> String.valueOf(i)).toArray(String[]::new);
//        System.out.println(StringUtils.join(',', roundArr));
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                for(int j = 0; j < round[0].length; j++){
                    JobInfo jobInfo = null;
                    try {
                        jobInfo = new JobInfo(3, algoName, "RUNNING");
                    } catch (UnknownHostException e) {
                        throw new RuntimeException(e);
                    }
                    jobInfo.addArgs("trainRatio", String.valueOf(trainRatio));
                    jobInfo.addArgs("testRatio", String.valueOf(testRatio));
                    jobInfo.addArgs("algoType", algoType);
                    jobInfo.addArgs("algoSubSetting", algoSubSetting);
                    jobInfo.addArgs("totalLoStartTime", System.currentTimeMillis() + "");
                    map.forEach(jobInfo::addAlgoArgs);
                    int jobId = jobService.createCombineJob(jobInfo,subJobNum);
                    jobService.createOrUpdateJobCountDown(jobId, 1);
                    //配置文件
                    String[] nodeIPList = new String[subJobNum];
                    ExecutorService es = Executors.newFixedThreadPool(subJobNum);
                    for (int i = 0; i < subJobNum; i++) {
                        //TODO:补充校验
                        int[] curExecutorsParams = Arrays.stream(executorsParamsList[i].split(" ")).mapToInt(Integer::parseInt).toArray();
                        String curExpParams = expParamsList[i];
                        int nodeId = algoInfos.data.get(i).getNodeId();
                        NodeInfo nodeInfo = nodeInfoService.queryForNodeInfoById(nodeId);
                        nodeIPList[i] = nodeInfo.getNameNodeIP();
                        String path = nodeInfo.getPrefix() + "globalrsp/" + algoInfos.data.get(i).getSuperName() + "/" + algoInfos.data.get(i).getGlobalrspName();
                        int finalJ = j;
                        int finalI = i;
                        Runnable runnable = new Runnable() {
                            @Override
                            public void run() {
                                AlgoDubboService algoDubboService = DubboUtils.getServiceRef(nodeInfo.getIp(), "com.szubd.rsp.algo.AlgoDubboService");
                                logger.info("发起调用，目标地址：{}， 目标服务：{}", nodeInfo.getIp(), "com.szubd.rsp.algo.AlgoDubboService");
                                try {
                                    algoDubboService.toAlgo(
                                            jobId,
                                            algoType,
                                            algoSubSetting,
                                            algoName,
                                            path,
                                            curExecutorsParams[0],
                                            curExecutorsParams[1],
                                            curExecutorsParams[2],
                                            curExpParams + " " + String.format("%.2f", round[finalI][finalJ]));
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        };
                        es.execute(runnable);
                    }
                    jobService.addJobArgs(jobId, "nodeIPList", StringUtils.join(':', nodeIPList));
                    jobService.syncInDB(jobId);
                    while(jobService.getJobCountDown(jobId) != 0){
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        };
        new Thread(runnable).start();
    }


    public void submitLogo(AlgoController.LogoAlgoInfos logoAlgoInfos, String algoType, String algoSubSetting, Map<String,String> map) throws URISyntaxException, UnknownHostException {
        String algoName = logoAlgoInfos.algo;
        int nodeId = logoAlgoInfos.getNodeId();
        NodeInfo nodeInfo = nodeInfoService.queryForNodeInfoById(nodeId);
        String path = nodeInfo.getPrefix() + "localrsp/" + logoAlgoInfos.getSuperName() + "/" + logoAlgoInfos.getName();
        AlgoDubboService algoDubboService = DubboUtils.getServiceRef(nodeInfo.getIp(), "com.szubd.rsp.algo.AlgoDubboService");

        try {
            algoDubboService.toAlgoLogo(
                                algoType,
                                algoSubSetting,
                                algoName,
                                path,
                                -1,
                                -1,
                                -1,
                                " "
                   );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public int uploadNewAlgo(AlgoInfo algoInfo) {
        return algoMapper.addAlgo(algoInfo);
    }

    public int upload(MultipartFile file){
        String tmpURI = "I://octopus_91/tmp/";
        //String tmpURI = "I:/study-code/uploadFile/files/";
        String localSavePath=tmpURI+file.getOriginalFilename();
        File localFile=new File(localSavePath);
        try {
            // 文件暂时保存在本地
            file.transferTo(localFile);
        } catch (IOException e) {
            log.error("[SPARK-INFO] 文件暂存本地失败");
            throw new RuntimeException(e);
        }
        List<String> ips = null;
        try {
            List<Instance> instances = nacosService.listAliveCluster();
            // 获取ip集合：查询当前存活节点
            ips = instances.stream().map(Instance::getIp).collect(Collectors.toList());
            //log.info("[SPARK-INFO] listAliveCluster:"+ips);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // String hdfsSavePath = "/tmp";
        String hdfsSavePath = "/user/hdfs/jars/";
        Executor executor = Executors.newFixedThreadPool(ips.size());
        // 创建多个 CompletableFuture 对象，每个对象对应一个 IP 地址进行文件并行化上传
        List<CompletableFuture<Void>> futures = ips.stream()
                .map(ip -> CompletableFuture.runAsync(() -> {
                    // 创建 Hadoop 配置对象
                    Configuration conf = new Configuration();
                    conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
                    //conf.set("fs.defaultFS", "hdfs://" + ip + ":8020/");
                    try {
                        // 创建 Hadoop 文件系统对象 设置上传用户为hdfs
                        FileSystem fs = FileSystem.get(new URI("hdfs://" + ip + ":8020/"),conf,"hdfs");
                        // hadoopAPI上传文件
                        Path fsSrcPath = new Path(localSavePath);
                        Path fsDstPath = new Path(hdfsSavePath);
                        fs.copyFromLocalFile(fsSrcPath, fsDstPath);
                        log.info("[SPARK-INFO] 文件上传至hdfs://" + ip + ":8020" + hdfsSavePath + "成功");
                        fs.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                        // HA状态切换ip（可以有更实用的方法 继续迭代）
                        if(ip.equals("172.31.238.102")){
                            conf.set("fs.defaultFS", "hdfs://" + "172.31.238.105" + ":8020/");
                            try {
                                // 创建 Hadoop 文件系统对象
                                FileSystem fs = FileSystem.get(conf);
                                // hadoopAPI上传文件
                                Path fsSrcPath = new Path(localSavePath);
                                Path fsDstPath = new Path(hdfsSavePath);
                                fs.copyFromLocalFile(fsSrcPath, fsDstPath);
                                log.info("[SPARK-INFO] 文件上传至hdfs://" + ip + ":8020" + hdfsSavePath + "成功");
                                fs.close();
                            }catch (Exception ex){
                                throw new RuntimeException(ex);
                            }
                        }
                    }
                }, executor)).collect(Collectors.toList());
        // 上传成功后返回响应并进行后续的删除中间暂存文件操作
        log.info("[SPARK-INFO] 主线程等待上传完成.......");
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        /*
        for(String ip:ips){
            // 创建 Hadoop 配置对象
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.defaultFS", "hdfs://"+ip+":8020/");
            try {
                // 创建 Hadoop 文件系统对象
                FileSystem fs = FileSystem.get(conf);
                // hadoopAPI上传文件
                Path fsSrcPath = new Path(localSavePath);
                Path fsDstPath = new Path(hdfsSavePath);
                fs.copyFromLocalFile(fsSrcPath,fsDstPath);
                log.info("文件上传至hdfs://"+ip+":8020"+hdfsSavePath+"成功");
                fs.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
*/
        CompletableFuture.runAsync(()->{
            log.info("[SPARK-INFO] 开始删除中间暂存文件");
            // 删除本地保存的文件
            if(localFile.delete()){
                log.info("[SPARK-INFO] 本地文件已删除成功！");
            }else{
                log.error("[SPARK-INFO] 本地文件删除失败！");
            }
        },executor);
        return 1;
    }

}
