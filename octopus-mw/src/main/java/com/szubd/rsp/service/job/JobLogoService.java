package com.szubd.rsp.service.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.szubd.rsp.algo.AlgoDubboService;
import com.szubd.rsp.job.JobDubboLogoService;

import com.szubd.rsp.job.JobInfo;
import com.szubd.rsp.job.JobLogoInfo;
import com.szubd.rsp.node.NodeInfo;
import com.szubd.rsp.node.NodeInfoService;
import com.szubd.rsp.resource.ClusterResourceService;
import com.szubd.rsp.resource.bean.ClusterInfo;
import com.szubd.rsp.resource.bean.ClusterMetrics;
import com.szubd.rsp.tools.DubboUtils;
import lombok.Data;
import org.apache.dubbo.config.annotation.DubboReference;
import org.apache.dubbo.config.annotation.DubboService;
import org.apache.dubbo.config.annotation.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

@Component
@DubboService
@Data
public class JobLogoService implements JobDubboLogoService {
    private int jobId = 0;
    private HashMap<Integer, JobLogoInfo> jobInfoHashMap = new HashMap<>();
    private HashMap<Integer, JobLogoInfo> endMap = new HashMap<>();
    private SimpleDateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    protected final static Logger logger = LoggerFactory.getLogger(JobLogoService.class);
    @Autowired
    private JobLogoInfoMapper jobLogoInfoMapper;
    @Autowired
    private NodeInfoService nodeInfoService;
//    @Reference
//    private ClusterResourceService clusterResourceService;

    //判断资源是否满足，满足则创建，不满足则失败
    synchronized public int createJob(JobLogoInfo jobInfo) {
//        boolean b = isValid(jobInfo);
//        if(!b) {
//
//            //进行Error操作并插入任务表
//        }
        //如果可以，往下执行
        int curJobId = ++jobId;
        logger.info("集群服务准备插入jobId为{}", curJobId);
        jobInfo.setJobId(curJobId);

        jobInfoHashMap.put(curJobId, jobInfo);
        Date date = new Date();
        jobInfo.setJobStartTime(dateFormat.format(date));
        try {
            persistInDB(jobInfo);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return curJobId;
    }

    public boolean isValid(int cores, int executors, int memory, int nodeId) {
//        //每个executor所需核数
//        int cores = jobInfo.getCores();
//        //几个executor
//        int executors = jobInfo.getExecutors();
//        //每个executor需要多少内存
//        int memory = jobInfo.getMemory();
//        AlgoDubboService algoDubboService = DubboUtils.getServiceRef(nodeInfo.getIp(), "com.szubd.rsp.algo.AlgoDubboService");
        //得到资源
//        clusterResourceService.getClusterMetricsInfo();
        NodeInfo nodeInfo = nodeInfoService.queryForNodeInfoById(nodeId);
        ClusterResourceService clusterResourceService = DubboUtils.getServiceRef(nodeInfo.getIp(), "com.szubd.rsp.resource.ClusterResourceService");
        ClusterMetrics clusterMetricsInfo = clusterResourceService.getClusterMetricsInfo();
        return !((long) executors * memory * 1024 >= clusterMetricsInfo.getAvailableMB()) && executors * cores < clusterMetricsInfo.getAvailableVirtualCores();
    }


    @Override
    public int updateJobStatus(int jobId, String jobStatus) {
        logger.info("id: {}, status: {}", jobId , jobStatus);
        JobLogoInfo jobInfo = jobInfoHashMap.get(jobId);
        if (jobInfo == null) {
            return 0;
        }
        jobInfo.setJobStatus(jobStatus);
        return 1;
    }

    @Override
    public int endJob(int jobId, String jobStatus) {
        logger.info("id: {}, end status: {}", jobId , jobStatus);
        JobLogoInfo jobInfo = jobInfoHashMap.get(jobId);
        if (jobInfo == null) {
            return 0;
        }
        jobInfo.setJobStatus(jobStatus);
        Date date = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        jobInfo.setJobEndTime(simpleDateFormat.format(date));
        syncInDB(jobId);
        return 1;
    }

    @Override
    public int updateJobArgs(int jobId, String argName, String argValue) {
        JobLogoInfo jobInfo = jobInfoHashMap.get(jobId);
        if (jobInfo == null) {
            return 0;
        }
        jobInfo.getArgs().put(argName, argValue);
        return 1;
    }

    @Override
    public JobLogoInfo getJobInfo(int jobId) {
        return jobInfoHashMap.get(jobId);
    }


    protected int persistInDB(JobLogoInfo jobInfo) throws JsonProcessingException {
        jobInfo.updateArgsJsonString();
        return jobLogoInfoMapper.insertJobInfo(jobInfo);
    }

    @Override
    public int syncInDB(int jobId) {
        JobLogoInfo jobInfo = jobInfoHashMap.get(jobId);
        if (jobInfo == null) {
            return 0;
        }
        try {
            jobInfo.updateArgsJsonString();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return jobLogoInfoMapper.updateJobInfo(jobInfo);
    }
    public void initJobId() {
        jobId = jobLogoInfoMapper.getAvaliableJobId() + 1;
        logger.info("当前JobId：{}", jobId);
    }

//    @Override
//    public int getAvaliableJobId() {
//        return 0;
//    }
}
