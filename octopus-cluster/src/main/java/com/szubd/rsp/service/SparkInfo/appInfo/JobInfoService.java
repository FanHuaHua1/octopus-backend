package com.szubd.rsp.service.SparkInfo.appInfo;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.szubd.rsp.sparkInfo.appInfo.JobInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.TreeSet;

import static com.szubd.rsp.tools.MakeUpUtils.*;
import static com.szubd.rsp.tools.SparkInfoHttpUtils.httpGetJobData;

/**
 * @projectName: octopus-web
 * @package: com.szubd.rsp.service.SparkInfo.appInfo
 * @className: JobInfoService
 * @author: Chandler
 * @description: JobInfo的方法：初始化、更新
 * @date: 2023/8/24 下午 2:39
 * @version: 1.0
 */
@Slf4j
@Component
public class JobInfoService {
    // 初始化JobInfo
    protected JobInfo initJobInfo(String parentEndpoint){
        JobInfo jobInfo = new JobInfo();
        TreeSet<JobInfo.JobData> succeedJob = new TreeSet<>();
        TreeSet<JobInfo.JobData> failedJob = new TreeSet<>();
        TreeSet<JobInfo.JobData> runningJob = new TreeSet<>();
        TreeSet<JobInfo.JobData> unknownJob = new TreeSet<>();
        jobInfo.setParentEndpoint(parentEndpoint);
        JSONArray jobJSONArray;
        // 获取数据
        try {
            jobJSONArray = httpGetJobData(parentEndpoint);
            jobInfo.setJobCount(jobJSONArray.size());
        } catch (Exception e) {
            log.error("[SPARK-INFO] " + "初始化JobInfo时无法从SparkHistoryServer中获取Job信息" + ", parentEndpoint: " +  parentEndpoint);
            jobInfo.setJobCount(0);
            return jobInfo;
        }
        // 填充数据：直接使用add方法即可，TreeSet会自动排序
        JobInfo.JobData jobData;
        for(Object o : jobJSONArray){
            int jobId = ((JSONObject) o).getIntValue("jobId");
            String status = ((JSONObject) o).getString("status");
            try {
                jobData = initJobData((JSONObject) o);
            } catch (Exception e) {
                log.error("[SPARK-INFO] " + "初始化JobInfo时获取JobData失败, jobId: " + jobId + ", parentEndpoint: " + parentEndpoint);
                jobData = new JobInfo.JobData();
            }
            if ("SUCCEEDED".equals(status)){
                succeedJob.add(jobData);
                jobInfo.setCurrentNums2Update(Math.max(jobInfo.getCurrentNums2Update(), jobId));
            } else if ("FAILED".equals(status)) {
                failedJob.add(jobData);
                jobInfo.setCurrentNums2Update(Math.max(jobInfo.getCurrentNums2Update(), jobId));
            } else if ("RUNNING".equals(status)) {
                runningJob.add(jobData);
            } else {
                unknownJob.add(jobData);
            }
        }
        // set方法更新数据
        jobInfo.setSucceedJob(succeedJob);
        jobInfo.setFailedJob(failedJob);
        jobInfo.setRunningJob(runningJob);
        jobInfo.setUnknownJob(unknownJob);
        jobInfo.setTimestamp(new Date(System.currentTimeMillis()));
        return jobInfo;
    }

    // 初始化内部类JobData方法
    private JobInfo.JobData initJobData(JSONObject o){
        JobInfo.JobData jobData = new JobInfo.JobData();
        jobData.setJobId(o.getInteger("jobId"));
        jobData.setName(o.getString("name"));
        jobData.setSubmissionTime(formatDateString(o.getString("submissionTime")));
        jobData.setCompletionTime(formatDateString(o.getString("completionTime")));
        jobData.setStageIds(makeUpString4StringWithBracket(o, "stageIds"));
        jobData.setStatus(o.getString("status"));
        jobData.setNumTasks(o.getIntValue("numTasks"));
        jobData.setNumActiveTasks(o.getIntValue("numActiveTasks"));
        jobData.setNumCompletedTasks(o.getIntValue("numCompletedTasks"));
        jobData.setNumSkippedTasks(o.getIntValue("numSkippedTasks"));
        jobData.setNumFailedTasks(o.getIntValue("numFailedTasks"));
        jobData.setNumKilledTasks(o.getIntValue("numKilledTasks"));
        jobData.setNumCompletedIndices(o.getIntValue("numCompletedIndices"));
        jobData.setNumActiveStages(o.getIntValue("numActiveStages"));
        if (jobData.getNumActiveStages() < 0) { jobData.setNumActiveStages(0); }
        jobData.setNumCompletedStages(o.getIntValue("numCompletedStages"));
        jobData.setNumSkippedStages(o.getIntValue("numSkippedStages"));
        jobData.setNumFailedStages(o.getIntValue("numFailedStages"));
        jobData.setNumStages(jobData.getNumActiveStages() +
                jobData.getNumCompletedStages() +
                jobData.getNumSkippedStages() +
                jobData.getNumFailedStages());
        jobData.setKilledTasksSummary(makeUpString4StringWithBracket(o,"killedTasksSummary"));
        return jobData;
    }

    // 根据时间戳判断是否需要更新JobInfo
    public JobInfo updateJobInfo(JobInfo jobInfo){
        if (jobInfo.getTimestamp()!=null &&
                !jobInfo.getTimestamp().before(new Date(System.currentTimeMillis()-GUARANTEE_TIME))){
            log.info("[SPARK-INFO] " + "JobData在保质期内，不需要更新");
            return jobInfo;
        }
        log.info("[SPARK-INFO] " + "JobData在保质期外，需要更新");
        // 获取信息
        JSONArray jobJSONArray;
        try {
            jobJSONArray = httpGetJobData(jobInfo.getParentEndpoint());
        } catch (Exception e) {
            log.error("[SPARK-INFO] " + "更新JobInfo时无法从SparkHistoryServer中获取Job信息" + ", parentEndpoint: " + jobInfo.getParentEndpoint());
            return jobInfo;
        }
        // 更新信息
        jobInfo.setJobCount(jobJSONArray.size());
        // 初始化Running的job集合（全量更新）
        TreeSet<JobInfo.JobData> runningJob0 = new TreeSet<>();
        // 获取增量更新的旧的集合
        TreeSet<JobInfo.JobData> succeedJob = jobInfo.getSucceedJob();
        TreeSet<JobInfo.JobData> failedJob = jobInfo.getFailedJob();
        TreeSet<JobInfo.JobData> unknownJob = jobInfo.getUnknownJob();
        JobInfo.JobData jobData;
        for(Object o: jobJSONArray){
            int jobId = ((JSONObject)o).getIntValue("jobId");
            String status = ((JSONObject) o).getString("status");
            // 获取Json信息从大到小获取，当获得Json的jobId信息小于等于当前的最大已完成jobId时不再需要更新
            if (jobId <= jobInfo.getCurrentNums2Update()){
                break;
            }
            try {
                jobData = initJobData((JSONObject) o);
            } catch (Exception e) {
                log.error("[SPARK-INFO] " + "更新JobInfo时获取JobData失败, jobId: " + jobId + ", parentEndpoint: " + jobInfo.getParentEndpoint());
                jobData = new JobInfo.JobData();
            }
            if ("SUCCEEDED".equals(status)){
                succeedJob.add(jobData);
            } else if ("FAILED".equals(status)) {
                failedJob.add(jobData);
            } else if ("RUNNING".equals(status)) {
                runningJob0.add(jobData);
            } else {
                unknownJob.add(jobData);
            }
        }
        // set方法更新数据
        jobInfo.setSucceedJob(succeedJob);
        jobInfo.setFailedJob(failedJob);
        jobInfo.setRunningJob(runningJob0);
        jobInfo.setUnknownJob(unknownJob);
        jobInfo.setTimestamp(new Date(System.currentTimeMillis()));
        return jobInfo;
    }
}
