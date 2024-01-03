package com.szubd.rsp.service.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.szubd.rsp.job.JobDubboService;
import com.szubd.rsp.job.JobInfo;
import com.szubd.rsp.service.integration.IntegrationService;
import javafx.util.Pair;
import lombok.Data;
import org.apache.dubbo.config.annotation.DubboService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import smile.classification.DecisionTree;

import java.sql.SQLOutput;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

@Component
@DubboService
@Data
public class JobService implements JobDubboService {
    private int jobId = 0;
    private HashMap<Integer, JobInfo> jobInfoHashMap = new HashMap<>();
    private HashMap<Integer, JobInfo> endMap = new HashMap<>();
    private HashMap<Integer, Integer> countDownMap = new HashMap<>();
    private SimpleDateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    protected final static Logger logger = LoggerFactory.getLogger(JobService.class);
    @Autowired
    private JobInfoMapper jobInfoMapper;
    public void addJobArgs(int jobId, String key, String value) {
        getJobInfo(jobId).addArgs(key, value);
    }

    synchronized public int createJob(JobInfo jobInfo) {
        int curJobId = ++jobId;
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

    @Override
    public synchronized int endSubJob(int mvJobId, String jobStatus) {
        getJobInfo(mvJobId).setJobStatus(jobStatus);
        HashMap<String, String> args = getJobInfo(getParentId(mvJobId)).getArgs();
        int subJobNum = Integer.parseInt(args.get("subJobNum"));
        if(subJobNum == 1){
            endJob(getParentId(mvJobId), "FINISHED");
        }
        args.put("subJobNum", String.valueOf(subJobNum - 1));
        getJobInfo(mvJobId).setJobEndTime(dateFormat.format(new Date()));
        syncInDB(mvJobId);
        return 1;
    }

    public synchronized int createCombineJob(JobInfo jobInfo, int subJobNum){
        int curJobId = ++jobId;
        jobInfo.setJobId(curJobId);
        jobInfo.setParentJobId(-1);
        jobInfo.getArgs().put("subJobNum", String.valueOf(subJobNum));
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

    /**
     * 更新任务状态，成功返回1，失败返回0
     * @param jobId
     * @param jobStatus
     * @return
     */
    public int updateJobStatus(int jobId, String jobStatus) {
        logger.info("id: {}, status: {}", jobId , jobStatus);
        JobInfo jobInfo = jobInfoHashMap.get(jobId);
        if (jobInfo == null) {
            return 0;
        }
        jobInfo.setJobStatus(jobStatus);
        return 1;
    }

    public int syncInDB(int jobId) {
        JobInfo jobInfo = jobInfoHashMap.get(jobId);
        if (jobInfo == null) {
            return 0;
        }
        try {
            jobInfo.updateArgsJsonString();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return jobInfoMapper.updateJobInfo(jobInfo);
    }

    protected int persistInDB( JobInfo jobInfo) throws JsonProcessingException {
        jobInfo.updateArgsJsonString();
        return jobInfoMapper.insertJobInfo(jobInfo);
    }

    @Override
    public int endJob(int jobId, String jobStatus) {
        logger.info("id: {}, end status: {}", jobId , jobStatus);
        JobInfo jobInfo = jobInfoHashMap.get(jobId);
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
        JobInfo jobInfo = jobInfoHashMap.get(jobId);
        if (jobInfo == null) {
            return 0;
        }
        jobInfo.getArgs().put(argName, argValue);
        return 1;
    }

    @Override
    public int updateMultiJobArgs(int jobId, String... kvs) {
        if(kvs.length == 0 || kvs.length % 2 != 0){
            return -1;
        }
        JobInfo jobInfo = jobInfoHashMap.get(jobId);
        if (jobInfo == null) {
            return 0;
        }
        HashMap<String, String> argsMap = jobInfo.getArgs();
        for(int i = 0; i < kvs.length; i+=2){
            argsMap.put(kvs[i], kvs[i + 1]);
        }
        return 1;
    }

    public JobInfo getJobInfo(int jobId) {
        return jobInfoHashMap.get(jobId);
    }

    @Override
    public int getParentId(int jobInfo) {
        return getJobInfo(jobInfo).getParentJobId();
    }

    public void addJob(JobInfo jobInfo) {
        jobInfoHashMap.put(jobId, jobInfo);
    }

    public void initJobId() {
        jobId = jobInfoMapper.getAvaliableJobId() + 1;
        logger.info("当前JobId：{}", jobId);
    }

    @Override
    public synchronized void createOrUpdateJobCountDown(int jobId, int count){
        countDownMap.put(jobId, count);
    }

    @Override
    public synchronized void reduceJobCountDown(int jobId){
        int count = countDownMap.get(jobId);
        countDownMap.put(jobId, count - 1);
    }
    @Override
    public synchronized int getJobCountDown(int jobId){
        return countDownMap.get(jobId);
    }

}
