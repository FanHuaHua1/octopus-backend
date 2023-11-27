package com.szubd.rsp.service.operation;

import com.szubd.rsp.algo.OperationDubboService;
import com.szubd.rsp.algo.RspMixParams;
import com.szubd.rsp.node.NodeInfoService;
import com.szubd.rsp.job.JobInfo;
import com.szubd.rsp.service.job.JobService;
import com.szubd.rsp.node.NodeInfo;
import com.szubd.rsp.tools.DubboUtils;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class RSPService {

    @Autowired
    private JobService jobService;
    @Autowired
    private NodeInfoService nodeInfoService;
    protected static final Logger logger = LoggerFactory.getLogger(RSPService.class);
    public void toRSPAction(String originName, String rspName, Integer blockNum, String originType, String nodeId) throws Exception {
        logger.info(" [RSPService] ToRSP job is preparing: {}", originName + ", " + rspName + ", " + blockNum + ", " + originType + ", " + nodeId);
        JobInfo jobInfo = new JobInfo(1, "toRsp", "PREPARE", -1);
        NodeInfo nodeInfo = nodeInfoService.queryForNodeInfoById(Integer.parseInt(nodeId));
        String ip = nodeInfo.getIp();
        HashMap<String, String> params = new HashMap<>();
        params.put("originName", originName);
        rspName = originName + "-" + rspName + "-" + System.currentTimeMillis();
        params.put("rspName", rspName);
        params.put("blockNum", blockNum.toString());
        params.put("ip", ip);
        jobInfo.setArgs(params);
        Date date = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        jobInfo.setJobStartTime(dateFormat.format(date));
        int jobId = jobService.createJob(jobInfo);
        OperationDubboService operationDubboService = DubboUtils.getServiceRef(ip, "com.szubd.rsp.algo.OperationDubboService");
//        ReferenceConfig<OperationDubboService> referenceConfig = new ReferenceConfig<>();
//        referenceConfig.setInterface("com.szubd.rsp.algo.OperationDubboService");
//        referenceConfig.setUrl("dubbo://"+ip+":20880/com.szubd.rsp.algo.OperationDubboService");
//        OperationDubboService operationDubboService = referenceConfig.get();
        operationDubboService.toRsp(jobId, originName, rspName, blockNum.toString(), originType);
    }

    public void rspMixAction(RspMixParams rspMixParams) throws URISyntaxException, IOException, InterruptedException {
        //混洗的节点数
        int mixNum = rspMixParams.data.size();
        //最终GlobalRsp的内层名字
        String fatherName = rspMixParams.data.get(0).getSuperName() + "-" + System.currentTimeMillis();
        JobInfo jobInfo = new JobInfo(2, "mixRsp", "RUNNING");
        jobInfo.addArgs("mixNum", String.valueOf(mixNum));
        jobInfo.addArgs("ratio", String.valueOf(rspMixParams.blockRatio));
        jobInfo.addArgs("fatherName", rspMixParams.data.get(0).getSuperName());
        jobInfo.addArgs("ip", Inet4Address.getLocalHost().getHostAddress());
        int jobId = jobService.createCombineJob(jobInfo, rspMixParams.data.size());
        JobInfo listJobInfo = new JobInfo(1, "generateList", "RUNNING", jobId);
        int listJobId = jobService.createJob(listJobInfo);
        //混洗节点比例,如2:3
        int[] ratioList = Arrays.stream(rspMixParams.blockRatio.split(":")).mapToInt(Integer::parseInt).toArray();
        assert ratioList.length == mixNum;
        //sum：混洗总块数
        int sum = Arrays.stream(ratioList).sum();
        //downLoadList:为每个块建立一个下载列表？
        ArrayList[] downLoadList = new ArrayList[sum];
        for (int i = 0; i < downLoadList.length; i++) {
            downLoadList[i] = new ArrayList<>();
        }
        String[] nodeIPList = new String[mixNum];
        //配置文件
        Configuration conf =  new Configuration();
        String user = "zhaolingxiang";
        //假设三个文件（包含多个块）进行混洗，则循环三次
        for (int i = 0; i < rspMixParams.data.size(); i++) {
            //这份数据的节点id
            int nodeId = rspMixParams.data.get(i).getNodeId();
            //这份数据的块数
            int blockNum = rspMixParams.data.get(i).getBlocks();
            //这份数据的节点信息
            NodeInfo nodeInfo = nodeInfoService.queryForNodeInfoById(nodeId);
            //这份数据的节点ip
            nodeIPList[i] = nodeInfo.getIp();
            //构造这份数据的FileStatus
            String url = "hdfs://" + nodeInfo.getNameNodeIP() + ":8020";
            String path = nodeInfo.getPrefix() + "origin/" + rspMixParams.data.get(i).getSuperName() + "/" + rspMixParams.data.get(i).getName();
            URI uri = new URI(url);
            FileSystem fileSystem = FileSystem.get(uri, conf, user);
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(path));
            //fileShuffleList：这份文件所有块的HDFS文件信息
            List<FileStatus> fileShuffleList = Arrays.asList(fileStatuses);
            assert fileShuffleList.size() >= sum;
            //打乱数据列表
            Collections.shuffle(fileShuffleList);
            for (int j = 0, k = 0; k < sum; j++, k++) {
                FileStatus fileStatus = fileShuffleList.get(j);
                if(fileStatus.getLen() == 0){
                    k--;
                    continue;
                }
                //为每个下载列表添加一个文件?
                downLoadList[k].add(fileStatus.getPath().toString());
            }
        }
        jobService.addJobArgs(jobId, "nodeIPList", StringUtils.join(':', nodeIPList));
        jobService.syncInDB(jobId);
        List<String>[][] finalList = new ArrayList[mixNum][];
        int startBound = 0;
        for (int i = 0; i < finalList.length; i++) {
            finalList[i] = new ArrayList[ratioList[i]];
            for (int k = startBound; k < startBound + ratioList[i]; k++) {
                finalList[i][k - startBound] = downLoadList[k];
            }
            startBound += ratioList[i];
        }
        ExecutorService es = Executors.newFixedThreadPool(mixNum);
        jobService.endJob(listJobId, "FINISHED");
        for (int i = 0; i < finalList.length; i++) {
            int finalI = i;
            Runnable runnable = () -> {
                JobInfo distcpJobInfo = null;
                try {
                    distcpJobInfo = new JobInfo(1, "distcp", "PREPARE", jobId);
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
                distcpJobInfo.addArgs("ip", nodeIPList[finalI]);
                int distcpJobId = jobService.createJob(distcpJobInfo);
                List<String>[] lists = finalList[finalI];
                ReferenceConfig<OperationDubboService> referenceConfig = new ReferenceConfig<>();
                referenceConfig.setInterface("com.szubd.rsp.algo.OperationDubboService");
                referenceConfig.setUrl("dubbo://"+nodeIPList[finalI]+":20880/com.szubd.rsp.algo.OperationDubboService");
                OperationDubboService operationDubboService = referenceConfig.get();
                try {
                    operationDubboService.toRspMix(finalList[finalI],
                            fatherName,
                            distcpJobId,
                            Integer.parseInt(rspMixParams.getRepartitionNum()),
                            rspMixParams.getMixType()
                    );
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            };
            es.execute(runnable);
        }
    }
}
