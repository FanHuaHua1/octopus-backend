package com.szubd.rsp.service.operation;

import com.szubd.rsp.algo.OperationDubboService;
import com.szubd.rsp.algo.RspMixParams;
import com.szubd.rsp.file.LocalRSPInfo;
import com.szubd.rsp.node.NodeInfoService;
import com.szubd.rsp.job.JobInfo;
import com.szubd.rsp.service.job.JobService;
import com.szubd.rsp.node.NodeInfo;
import com.szubd.rsp.tools.DubboUtils;
import javafx.util.Pair;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.spark.sql.sources.In;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Int;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.sql.Array;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        operationDubboService.toRsp(jobId, originName, rspName, blockNum.toString(), originType);
    }

    public void rspMixAction(RspMixParams<LocalRSPInfo> rspMixParams) throws URISyntaxException, IOException, InterruptedException {
        //混洗的节点数
        int mixNum = rspMixParams.data.size();
        //最终GlobalRsp的内层名字
        String fatherName = rspMixParams.data.get(0).getSuperName() + "-" + System.currentTimeMillis();
        JobInfo jobInfo = new JobInfo(2, "mixRsp", "RUNNING");
        jobInfo.addMultiArgs(
                "mixNum", String.valueOf(mixNum),
                "ratio", String.valueOf(rspMixParams.blockRatio),
                "fatherName", rspMixParams.data.get(0).getSuperName(),
                "ip", Inet4Address.getLocalHost().getHostAddress()
        );
        int jobId = jobService.createCombineJob(jobInfo, rspMixParams.data.size());
        JobInfo listJobInfo = new JobInfo(1, "generateList", "RUNNING", jobId);
        int listJobId = jobService.createJob(listJobInfo);
        //混洗节点块数,如2:2
        int[] ratioList = Arrays.stream(rspMixParams.blockRatio.split(":")).mapToInt(Integer::parseInt).toArray();
        assert ratioList.length == mixNum;
        //sum：混洗总块数 对于2:2 sum = 4
        int sum = Arrays.stream(ratioList).sum();
        //downLoadList:为每个块建立一个下载列表？
        ArrayList[] downLoadList = new ArrayList[sum];
        for (int i = 0; i < downLoadList.length; i++) {
            downLoadList[i] = new ArrayList<>();
        }
        String[] nodeIPList = new String[mixNum];
        //配置文件
        Configuration conf =  new Configuration();
        String user = "bigdata";
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
            String path = nodeInfo.getPrefix() + "localrsp/" + rspMixParams.data.get(i).getSuperName() + "/" + rspMixParams.data.get(i).getName();
            URI uri = new URI(url);
            FileSystem fileSystem = FileSystem.get(uri, conf, user);
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(path));
            //fileShuffleList：这份文件所有块的HDFS文件信息
            List<FileStatus> fileShuffleList = Arrays.asList(fileStatuses);
            assert fileShuffleList.size() >= sum;
            //打乱数据列表
            Collections.shuffle(fileShuffleList);
            //k是用来防止fileShuffleList中有0字节的文件（如标志文件SUCCESS_）
            //j是用来遍历fileShuffleList
            //结束之后，downLoadList[i]中都会有各个文件的一个块
            //***********************************************
            //这里的思想是，把每一个文件的块打乱之后，分别先放一个到downLoadList[i]中
            //这样，每个downLoadList[i]都是一个全局RSP块，里面是各个文件块叠加的，后续要合成一个文件，并且考虑到大小问题，要进行重新分区
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
            //k是记录downLoadList分配到哪个索引了，比如2:2分配，索引0到1会给到ip1，2-4会给到ip2
            for (int k = startBound; k < startBound + ratioList[i]; k++) {
                finalList[i][k - startBound] = downLoadList[k];
            }
            startBound += ratioList[i];
        }
        ExecutorService es = Executors.newFixedThreadPool(mixNum);
        jobService.endJob(listJobId, "FINISHED");
        List<List<Pair<Integer, Integer>>> shuffleTaskSeq = getShuffleTaskSeq(nodeIPList.length);
        String[][] tasks = new String[nodeIPList.length][shuffleTaskSeq.size()];
        for (int i = 0; i < shuffleTaskSeq.size(); i++){
            System.out.println("第" + i + "轮：");
            for (Pair<Integer, Integer> pair : shuffleTaskSeq.get(i)) {
                System.out.print(pair.getKey() + "->" + pair.getValue() + " ");
                tasks[pair.getValue()][i] = nodeIPList[pair.getKey()];
            }
        }
        //分阶段：
        //第一阶段：先把数据传输完

        new Thread(() -> {
            logger.info(" [RSPService (AT ONCE)] Starting transfer across domain");
            long startTime = System.nanoTime();
            jobService.createOrUpdateJobCountDown(jobId, finalList.length * 2);
            for (int i = 0; i < finalList.length; i++) {
                int finalI = i;
                Stream<String> stringStream = Arrays.stream(finalList[finalI]).flatMap(List::stream);
                //Stream<String> stringStream = fileList.stream().flatMap(List::stream);
                List<String> totalList = stringStream.collect(Collectors.toList());
                Runnable runnable = () -> {
                    JobInfo distcpJobInfo = null;
                    distcpJobInfo = new JobInfo(1, "distcp", "PREPARE", nodeIPList[finalI] ,jobId);
                    distcpJobInfo.addArgs("ip", nodeIPList[finalI]);
                    int distcpJobId = jobService.createJob(distcpJobInfo);
                    OperationDubboService operationDubboService = DubboUtils.getServiceRef(nodeIPList[finalI], "com.szubd.rsp.algo.OperationDubboService");
                    try {
                        operationDubboService.distcpBeforeMix(
                                totalList,
                                tasks[finalI],
                                fatherName,
                                distcpJobId,
                                jobId
                        );
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                };
                es.execute(runnable);
            }
            while(jobService.getJobCountDown(jobId) != finalList.length){
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            double transferDuration = (System.nanoTime() - startTime)  * 0.000000001;;
            logger.info(" [RSPService (AT ONCE)] Transfer across domain over, duration: {}s", transferDuration);
            jobService.updateJobArgs(jobId,
                    "transfer-duration", String.valueOf(transferDuration));
            jobService.syncInDB(jobId);
            long transferStartTime = System.nanoTime();
            for (int i = 0; i < finalList.length; i++) {
                int finalI = i;
                Runnable runnable = () -> {
                    OperationDubboService operationDubboService = DubboUtils.getServiceRef(nodeIPList[finalI], "com.szubd.rsp.algo.OperationDubboService");
                    try {
                        operationDubboService.rspMerge(Arrays.asList(finalList[finalI]),
                                fatherName,
                                jobId,
                                Integer.parseInt(rspMixParams.getRepartitionNum()),
                                rspMixParams.getMixType()
                        );
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                };
                es.execute(runnable);
            }
            while(jobService.getJobCountDown(jobId) != 0){
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            double mergeDuration = (System.nanoTime() - transferStartTime) * 0.000000001;
            logger.info(" [RSPService (AT ONCE)] rsp merge across domain over, duration: {}s", mergeDuration);
            jobService.updateJobArgs(jobId,
                    "merge-duration", String.valueOf(mergeDuration));
            jobService.syncInDB(jobId);
        }).start();
    }

    /**
     * 实验用
     * @param rspMixParams
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public void collectExp(RspMixParams<LocalRSPInfo> rspMixParams) throws URISyntaxException, IOException, InterruptedException {
        //混洗的节点数
        int mixNum = rspMixParams.data.size();
        //最终GlobalRsp的内层名字
        String fatherName = rspMixParams.data.get(0).getSuperName() + "-" + System.currentTimeMillis();
        JobInfo jobInfo = new JobInfo(2, "mixRsp", "RUNNING");
        jobInfo.addMultiArgs(
                "mixNum", String.valueOf(mixNum),
                "ratio", String.valueOf(rspMixParams.blockRatio),
                "fatherName", rspMixParams.data.get(0).getSuperName(),
                "ip", Inet4Address.getLocalHost().getHostAddress()
        );
        int jobId = jobService.createCombineJob(jobInfo, rspMixParams.data.size());
        JobInfo listJobInfo = new JobInfo(1, "generateList", "RUNNING", jobId);
        int listJobId = jobService.createJob(listJobInfo);
        //混洗节点块数,如2:2
        int[] ratioList = Arrays.stream(rspMixParams.blockRatio.split(":")).mapToInt(Integer::parseInt).toArray();
        assert ratioList.length == mixNum;
        //sum：混洗总块数 对于2:2 sum = 4
        int sum = Arrays.stream(ratioList).sum();
        //downLoadList:为每个块建立一个下载列表？
        ArrayList[] downLoadList = new ArrayList[sum];
        for (int i = 0; i < downLoadList.length; i++) {
            downLoadList[i] = new ArrayList<>();
        }
        String[] nodeIPList = new String[mixNum];
        //配置文件
        Configuration conf =  new Configuration();
        String user = "bigdata";
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
            String path = nodeInfo.getPrefix() + "localrsp/" + rspMixParams.data.get(i).getSuperName() + "/" + rspMixParams.data.get(i).getName();
            URI uri = new URI(url);
            FileSystem fileSystem = FileSystem.get(uri, conf, user);
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(path));
            //fileShuffleList：这份文件所有块的HDFS文件信息
            List<FileStatus> fileShuffleList = Arrays.asList(fileStatuses);
            assert fileShuffleList.size() >= sum;
            //打乱数据列表
            Collections.shuffle(fileShuffleList);
            //k是用来防止fileShuffleList中有0字节的文件（如标志文件SUCCESS_）
            //j是用来遍历fileShuffleList
            //结束之后，downLoadList[i]中都会有各个文件的一个块
            //***********************************************
            //这里的思想是，把每一个文件的块打乱之后，分别先放一个到downLoadList[i]中
            //这样，每个downLoadList[i]都是一个全局RSP块，里面是各个文件块叠加的，后续要合成一个文件，并且考虑到大小问题，要进行重新分区
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
            //k是记录downLoadList分配到哪个索引了，比如2:2分配，索引0到1会给到ip1，2-4会给到ip2
            for (int k = startBound; k < startBound + ratioList[i]; k++) {
                finalList[i][k - startBound] = downLoadList[k];
            }
            startBound += ratioList[i];
        }
        ExecutorService es = Executors.newFixedThreadPool(mixNum);
        jobService.endJob(listJobId, "FINISHED");
        List<List<Pair<Integer, Integer>>> shuffleTaskSeq = getShuffleTaskSeq(nodeIPList.length);
        String[][] tasks = new String[nodeIPList.length][shuffleTaskSeq.size()];
        for (int i = 0; i < shuffleTaskSeq.size(); i++){
            System.out.println("第" + i + "轮：");
            for (Pair<Integer, Integer> pair : shuffleTaskSeq.get(i)) {
                System.out.print(pair.getKey() + "->" + pair.getValue() + " ");
                tasks[pair.getValue()][i] = nodeIPList[pair.getKey()];
            }
        }
        //分阶段：
        //第一阶段：先把数据传输完

        new Thread(() -> {
            logger.info(" [RSPService (AT ONCE)] Starting transfer across domain");
            long startTime = System.nanoTime();
            jobService.createOrUpdateJobCountDown(jobId, 1);
            for (int i = 0; i < finalList.length; i++) {
                int finalI = i;
                if(!"192.168.0.67".equals(nodeIPList[finalI])){
                    continue;
                }
                Stream<String> stringStream = Arrays.stream(finalList[finalI]).flatMap(List::stream);
                //Stream<String> stringStream = fileList.stream().flatMap(List::stream);
                List<String> totalList = stringStream.collect(Collectors.toList());
                Runnable runnable = () -> {
                    JobInfo distcpJobInfo = null;
                    distcpJobInfo = new JobInfo(1, "distcp", "PREPARE", nodeIPList[finalI] ,jobId);
                    distcpJobInfo.addArgs("ip", nodeIPList[finalI]);
                    int distcpJobId = jobService.createJob(distcpJobInfo);
                    OperationDubboService operationDubboService = DubboUtils.getServiceRef(nodeIPList[finalI], "com.szubd.rsp.algo.OperationDubboService");
                    try {
                        operationDubboService.distcpBeforeMix(
                                totalList,
                                tasks[finalI],
                                fatherName,
                                distcpJobId,
                                jobId
                        );
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                };
                es.execute(runnable);
            }
            while(jobService.getJobCountDown(jobId) != 0){
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            double transferDuration = (System.nanoTime() - startTime)  * 0.000000001;;
            logger.info(" [RSPService (AT ONCE)] Transfer across domain over, duration: {}s", transferDuration);
            jobService.updateJobArgs(jobId,
                    "transfer-duration", String.valueOf(transferDuration));
            jobService.syncInDB(jobId);
        }).start();
    }

    private List<List<Pair<Integer, Integer>>> getShuffleTaskSeq(int len){
        List<List<Pair<Integer, Integer>>> arr_out = new ArrayList<>();
        for (int i = 0; i < len / 2; i++) {
            List<Pair<Integer, Integer>> tuplesList = new ArrayList<>();
            for (int j = 0; j < len; j++) {
                Pair<Integer, Integer> transferPair = new Pair<>(j, j + i + 1 < len ? j + i + 1 : i + j + 1 - len);
                tuplesList.add(transferPair);
            }
            arr_out.add(tuplesList);
            List<Pair<Integer, Integer>> listReverse = judgeReverse(tuplesList);
            if(!listReverse.isEmpty()){
                arr_out.add(listReverse);
            }
        }
        //打印arr_out
        for (List<Pair<Integer, Integer>> pairs : arr_out) {
            System.out.println("第" + arr_out.indexOf(pairs) + "轮：");
            for (Pair<Integer, Integer> pair : pairs) {
                System.out.print(pair.getKey() + "->" + pair.getValue() + " ");
            }
            System.out.println();
        }
        return arr_out;
    }

    private List<Pair<Integer, Integer>> judgeReverse(List<Pair<Integer, Integer>> list){
        List<Pair<Integer, Integer>> listReverse = new ArrayList<>();
        for (Pair<Integer, Integer> pair : list) {
            Pair<Integer, Integer> reversePair = new Pair<>(pair.getValue(), pair.getKey());
            if (!list.contains(reversePair)) {
                listReverse.add(reversePair);
            }
        }
        return listReverse;
    }

    public void rspMixActionWithStrategy(RspMixParams<LocalRSPInfo> rspMixParams) throws URISyntaxException, IOException, InterruptedException {
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
        //混洗节点块数,如2:2
        int[] ratioList = Arrays.stream(rspMixParams.blockRatio.split(":")).mapToInt(Integer::parseInt).toArray();
        assert ratioList.length == mixNum;
        //sum：混洗总块数 对于2:2 sum = 4
        int sum = Arrays.stream(ratioList).sum();
        //downLoadList:为每个块建立一个下载列表？
        ArrayList<Pair<String, String>>[] downLoadList = new ArrayList[sum];
        for (int i = 0; i < downLoadList.length; i++) {
            downLoadList[i] = new ArrayList<>();
        }
        String[] nodeIPList = new String[mixNum];
        //配置文件
        Configuration conf =  new Configuration();
        String user = "bigdata";
        //假设三个文件（包含多个块）进行混洗，则循环三次
        Map<String, Integer> ipMap = new HashMap<>();
        for (int i = 0; i < rspMixParams.data.size(); i++) {
            //这份数据的节点id
            int nodeId = rspMixParams.data.get(i).getNodeId();
            //这份数据的块数
            int blockNum = rspMixParams.data.get(i).getBlocks();
            //这份数据的节点信息
            NodeInfo nodeInfo = nodeInfoService.queryForNodeInfoById(nodeId);
            //这份数据的节点ip
            nodeIPList[i] = nodeInfo.getIp();
            ipMap.put(nodeIPList[i], i);
            //构造这份数据的FileStatus
            String url = "hdfs://" + nodeInfo.getNameNodeIP() + ":8020";
            String path = nodeInfo.getPrefix() + "localrsp/" + rspMixParams.data.get(i).getSuperName() + "/" + rspMixParams.data.get(i).getName();
            URI uri = new URI(url);
            FileSystem fileSystem = FileSystem.get(uri, conf, user);
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(path));
            //fileShuffleList：这份文件所有块的HDFS文件信息
            List<FileStatus> fileShuffleList = Arrays.asList(fileStatuses);
            assert fileShuffleList.size() >= sum;
            //打乱数据列表
            Collections.shuffle(fileShuffleList);
            //k是用来防止fileShuffleList中有0字节的文件（如标志文件SUCCESS_）
            //j是用来遍历fileShuffleList
            //结束之后，downLoadList[i]中都会有各个文件的一个块
            //***********************************************
            //这里的思想是，把每一个文件的块打乱之后，分别先放一个到downLoadList[i]中
            //这样，每个downLoadList[i]都是一个全局RSP块，里面是各个文件块叠加的，后续要合成一个文件，并且考虑到大小问题，要进行重新分区
            for (int j = 0, k = 0; k < sum; j++, k++) {
                FileStatus fileStatus = fileShuffleList.get(j);
                if(fileStatus.getLen() == 0){
                    k--;
                    continue;
                }
                //为每个下载列表添加一个文件?
                //downLoadList[k].add(fileStatus.getPath().toString());
                downLoadList[k].add(new Pair<>(nodeInfo.getIp(), fileStatus.getPath().toString()));
            }
        }
        jobService.addJobArgs(jobId, "nodeIPList", StringUtils.join(':', nodeIPList));
        jobService.syncInDB(jobId);
        List<Pair<String, String>>[][] finalList = new ArrayList[mixNum][];
        int startBound = 0;
        for (int i = 0; i < finalList.length; i++) {
            finalList[i] = new ArrayList[ratioList[i]];
            //k是记录downLoadList分配到哪个索引了，比如2:2分配，索引0到1会给到ip1，2-4会给到ip2
            for (int k = startBound; k < startBound + ratioList[i]; k++) {
                finalList[i][k - startBound] = downLoadList[k];
            }
            startBound += ratioList[i];
        }
        //打印finnallist
        //System.out.println("打印finnallist==========================");
//        for (int i = 0; i < finalList.length; i++) {
//            System.out.println("第" + i + "个节点：");
//            for (int j = 0; j < finalList[i].length; j++) {
//                System.out.println("第" + j + "个块：");
//                for (Pair<String, String> pair : finalList[i][j]) {
//                    System.out.println(pair.getKey() + " " + pair.getValue());
//                }
//            }
//        }
        jobService.endJob(listJobId, "FINISHED");
        Map<String, Map<String, List<String>>> transferTasks = new HashMap<>();
        for (int i = 0; i < nodeIPList.length; i++) {
            Map<String, List<String>> map = new HashMap<>();
            Arrays.stream(finalList[i]).flatMap(Collection::stream).forEach(pair -> {
                String ip = pair.getKey();
                String path = pair.getValue();
                if(!map.containsKey(ip)){
                    map.put(ip, new ArrayList<>());
                }
                map.get(ip).add(path);
            });
            transferTasks.put(nodeIPList[i], map);
        }
        //打印transferTasks
//        System.out.println("打印transferTasks==========================");
//        for (Map.Entry<String, Map<String, List<String>>> entry : transferTasks.entrySet()) {
//            System.out.println("ip: " + entry.getKey());
//            for (Map.Entry<String, List<String>> entry1 : entry.getValue().entrySet()) {
//                System.out.println("send: " + entry1.getKey());
//                for (String s : entry1.getValue()) {
//                    System.out.println("recv: " + s);
//                }
//            }
//        }
        List<List<Pair<Integer, Integer>>> shuffleTaskSeq = getShuffleTaskSeq(nodeIPList.length);
        List<Map<String, List<String>>> totalTasks = new ArrayList<>();
        for (List<Pair<Integer, Integer>> pairs : shuffleTaskSeq) {
            Map<String, List<String>> curTask = new HashMap<>();
            for (Pair<Integer, Integer> pair : pairs) {
                String sendIP = nodeIPList[pair.getKey()];
                String recvIP = nodeIPList[pair.getValue()];
                curTask.put(recvIP, transferTasks.get(recvIP).get(sendIP));
            }
            totalTasks.add(curTask);
        }
        Map<String, List<String>> loopBackTasks = new HashMap<>();
        for (String ip : nodeIPList) {
            loopBackTasks.put(ip, transferTasks.get(ip).get(ip));
        }
        totalTasks.add(loopBackTasks);
        new Thread(() -> {
            //第一阶段：distcp
            logger.info(" [RSPService] Starting transfer across domain");
            long startTime = System.currentTimeMillis();
            for (Map<String, List<String>> totalTask : totalTasks) {
                //每一轮有多少个传输
                jobService.createOrUpdateJobCountDown(jobId, totalTask.size());
                for (Map.Entry<String, List<String>> entry : totalTask.entrySet()) {
                    logger.info(" [RSPService] Distcp job is starting: {}", entry.getKey() + ", " + entry.getValue());
                    new Thread(() -> {
                        JobInfo distcpJobInfo = null;
                        try {
                            distcpJobInfo = new JobInfo(1, "distcp", "PREPARE", jobId);
                        } catch (UnknownHostException e) {
                            throw new RuntimeException(e);
                        }
                        String ip = entry.getKey();
                        List<String> paths = entry.getValue();
                        distcpJobInfo.addArgs("ip", ip);
                        int distcpJobId = jobService.createJob(distcpJobInfo);
                        OperationDubboService operationDubboService = DubboUtils.getServiceRef(ip, "com.szubd.rsp.algo.OperationDubboService");
                        try {
                            operationDubboService.distcpBeforeMix(
                                    paths,
                                    null,
                                    fatherName,
                                    distcpJobId,
                                    jobId
                            );
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }).start();
                }
                while(jobService.getJobCountDown(jobId) != 0){
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            long endTime = System.currentTimeMillis();
            logger.info(" [RSPService] Transfer across domain over, duration: {}s", 1.0 * (endTime - startTime) / 1000);
            //第一阶段：merge
            for (int i = 0; i < finalList.length; i++) {
               int finalI = i;
               new Thread(() -> {
                   OperationDubboService operationDubboService = DubboUtils.getServiceRef(nodeIPList[finalI], "com.szubd.rsp.algo.OperationDubboService");
                    Stream<List<String>> listStream = Arrays.stream(finalList[finalI]).map(list -> list.stream().map(elem -> elem.getValue()).collect(Collectors.toList()));
                    List<List<String>> lists = listStream.collect(Collectors.toList());
                   try {
                        operationDubboService.rspMerge(lists,
                                fatherName,
                                jobId,
                                Integer.parseInt(rspMixParams.getRepartitionNum()),
                                rspMixParams.getMixType()
                        );
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).start();
            }
        }).start();
    }

    public void rspMixActionWithStrategy2(RspMixParams<LocalRSPInfo> rspMixParams) throws URISyntaxException, IOException, InterruptedException {
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
        //混洗节点块数,如2:2
        int[] ratioList = Arrays.stream(rspMixParams.blockRatio.split(":")).mapToInt(Integer::parseInt).toArray();
        assert ratioList.length == mixNum;
        //sum：混洗总块数 对于2:2 sum = 4
        int sum = Arrays.stream(ratioList).sum();
        //downLoadList:为每个块建立一个下载列表
        //注意，这里downLoadList[i]就是一个全局RSP块，其中记录着来着各个节点的单个块，所以这里downLoadList的长度就是总的块数
        ArrayList<Pair<String, String>>[] downLoadList = new ArrayList[sum];
        for (int i = 0; i < downLoadList.length; i++) {
            downLoadList[i] = new ArrayList<>();
        }
        String[] nodeIPList = new String[mixNum];
        //配置文件
        Configuration conf =  new Configuration();
        String user = "bigdata";
        //假设三个文件（包含多个块）进行混洗，则循环三次
        Map<String, Integer> ipMap = new HashMap<>();
        for (int i = 0; i < rspMixParams.data.size(); i++) {
            //这份数据的节点id
            int nodeId = rspMixParams.data.get(i).getNodeId();
            //这份数据的块数
            int blockNum = rspMixParams.data.get(i).getBlocks();
            //这份数据的节点信息
            NodeInfo nodeInfo = nodeInfoService.queryForNodeInfoById(nodeId);
            //这份数据的节点ip
            nodeIPList[i] = nodeInfo.getIp();
            ipMap.put(nodeIPList[i], i);
            //构造这份数据的FileStatus
            String url = "hdfs://" + nodeInfo.getNameNodeIP() + ":8020";
            String path = nodeInfo.getPrefix() + "localrsp/" + rspMixParams.data.get(i).getSuperName() + "/" + rspMixParams.data.get(i).getName();
            URI uri = new URI(url);
            FileSystem fileSystem = FileSystem.get(uri, conf, user);
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(path));
            //fileShuffleList：这份文件所有块的HDFS文件信息
            List<FileStatus> fileShuffleList = Arrays.asList(fileStatuses);
            assert fileShuffleList.size() >= sum;
            //打乱数据列表
            Collections.shuffle(fileShuffleList);
            //k是用来防止fileShuffleList中有0字节的文件（如标志文件SUCCESS_）
            //j是用来遍历fileShuffleList
            //结束之后，downLoadList[i]中都会有各个文件的一个块
            //***********************************************
            //这里的思想是，把每一个文件的块打乱之后，分别先放一个到downLoadList[i]中
            //这样，每个downLoadList[i]都是一个全局RSP块，里面是各个文件块叠加的，后续要合成一个文件，并且考虑到大小问题，要进行重新分区
            for (int j = 0, k = 0; k < sum; j++, k++) {
                FileStatus fileStatus = fileShuffleList.get(j);
                if(fileStatus.getLen() == 0){
                    k--;
                    continue;
                }
                //为每个下载列表添加一个文件?
                //downLoadList[k].add(fileStatus.getPath().toString());
                downLoadList[k].add(new Pair<>(nodeInfo.getIp(), fileStatus.getPath().toString()));
            }
        }
        jobService.addJobArgs(jobId, "nodeIPList", StringUtils.join(':', nodeIPList));
        jobService.syncInDB(jobId);
        List<Pair<String, String>>[][] finalList = new ArrayList[mixNum][];
        int startBound = 0;
        for (int i = 0; i < finalList.length; i++) {
            //k是记录downLoadList分配到哪个索引了，
            // 比如2:2分配，那么finalList的长度就是2，downLoadList索引0到1会给到ip1，2-4会给到ip2
            finalList[i] = new ArrayList[ratioList[i]];
            for (int k = startBound; k < startBound + ratioList[i]; k++) {
                finalList[i][k - startBound] = downLoadList[k];
            }
            startBound += ratioList[i];
        }
        //打印finnallist
        //System.out.println("打印finnallist==========================");
//        for (int i = 0; i < finalList.length; i++) {
//            System.out.println("第" + i + "个节点：");
//            for (int j = 0; j < finalList[i].length; j++) {
//                System.out.println("第" + j + "个块：");
//                for (Pair<String, String> pair : finalList[i][j]) {
//                    System.out.println(pair.getKey() + " " + pair.getValue());
//                }
//            }
//        }
        jobService.endJob(listJobId, "FINISHED");
        Map<String, Map<String, List<String>>> transferTasks = new HashMap<>();
        for (int i = 0; i < nodeIPList.length; i++) {
            Map<String, List<String>> map = new HashMap<>();
            Arrays.stream(finalList[i]).flatMap(Collection::stream).forEach(pair -> {
                String ip = pair.getKey();
                String path = pair.getValue();
                if(!map.containsKey(ip)){
                    map.put(ip, new ArrayList<>());
                }
                map.get(ip).add(path);
            });
            transferTasks.put(nodeIPList[i], map);
        }
        //打印transferTasks
//        System.out.println("打印transferTasks==========================");
//        for (Map.Entry<String, Map<String, List<String>>> entry : transferTasks.entrySet()) {
//            System.out.println("ip: " + entry.getKey());
//            for (Map.Entry<String, List<String>> entry1 : entry.getValue().entrySet()) {
//                System.out.println("send: " + entry1.getKey());
//                for (String s : entry1.getValue()) {
//                    System.out.println("recv: " + s);
//                }
//            }
//        }
        List<List<Pair<Integer, Integer>>> shuffleTaskSeq = getShuffleTaskSeq(nodeIPList.length);
        List<Map<String, List<String>>> totalTasks = new ArrayList<>();
        for (List<Pair<Integer, Integer>> pairs : shuffleTaskSeq) {
            Map<String, List<String>> curTask = new HashMap<>();
            for (Pair<Integer, Integer> pair : pairs) {
                String sendIP = nodeIPList[pair.getKey()];
                String recvIP = nodeIPList[pair.getValue()];
                curTask.put(recvIP, transferTasks.get(recvIP).get(sendIP));
            }
            totalTasks.add(curTask);
        }
        Map<String, List<String>> loopBackTasks = new HashMap<>();
        for (String ip : nodeIPList) {
            loopBackTasks.put(ip, transferTasks.get(ip).get(ip));
        }
        totalTasks.add(loopBackTasks);
        new Thread(() -> {
            //第一阶段：distcp
            logger.info(" [RSPService] Starting transfer across domain");
            long startTime = System.currentTimeMillis();
            for (Map<String, List<String>> totalTask : totalTasks) {
                //每一轮有多少个传输
                jobService.createOrUpdateJobCountDown(jobId, totalTask.size());
                for (Map.Entry<String, List<String>> entry : totalTask.entrySet()) {
                    logger.info(" [RSPService] Distcp job is starting: {}", entry.getKey() + ", " + entry.getValue());
                    new Thread(() -> {
                        JobInfo distcpJobInfo = null;
                        try {
                            distcpJobInfo = new JobInfo(1, "distcp", "PREPARE", jobId);
                        } catch (UnknownHostException e) {
                            throw new RuntimeException(e);
                        }
                        String ip = entry.getKey();
                        List<String> paths = entry.getValue();
                        distcpJobInfo.addArgs("ip", ip);
                        int distcpJobId = jobService.createJob(distcpJobInfo);
                        OperationDubboService operationDubboService = DubboUtils.getServiceRef(ip, "com.szubd.rsp.algo.OperationDubboService");
                        try {
                            operationDubboService.distcpBeforeMix(
                                    paths,
                                    null,
                                    fatherName,
                                    distcpJobId,
                                    jobId
                            );
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }).start();
                }
                while(jobService.getJobCountDown(jobId) != 0){
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            long endTime = System.currentTimeMillis();
            logger.info(" [RSPService] Transfer across domain over, duration: {}s", 1.0 * (endTime - startTime) / 1000);
            //第一阶段：merge
            for (int i = 0; i < finalList.length; i++) {
                int finalI = i;
                new Thread(() -> {
                    OperationDubboService operationDubboService = DubboUtils.getServiceRef(nodeIPList[finalI], "com.szubd.rsp.algo.OperationDubboService");
                    Stream<List<String>> listStream = Arrays.stream(finalList[finalI]).map(list -> list.stream().map(elem -> elem.getValue()).collect(Collectors.toList()));
                    List<List<String>> lists = listStream.collect(Collectors.toList());
                    try {
                        operationDubboService.rspMerge(lists,
                                fatherName,
                                jobId,
                                Integer.parseInt(rspMixParams.getRepartitionNum()),
                                rspMixParams.getMixType()
                        );
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).start();
            }
        }).start();
    }
}
