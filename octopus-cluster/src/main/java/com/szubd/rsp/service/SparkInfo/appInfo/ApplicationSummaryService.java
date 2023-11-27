package com.szubd.rsp.service.SparkInfo.appInfo;

import com.alibaba.fastjson.JSONObject;
import com.szubd.rsp.sparkInfo.appInfo.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Date;

import static com.szubd.rsp.tools.MakeUpUtils.GUARANTEE_TIME;
import static com.szubd.rsp.tools.MakeUpUtils.formatDateString;
import static com.szubd.rsp.tools.SparkInfoHttpUtils.httpGetAppData;

/**
 * @projectName: octopus-web
 * @package: com.szubd.rsp.service.SparkInfo.appInfo
 * @className: ApplicationSummaryService
 * @author: Chandler
 * @description: ApplicationSummary的方法：初始化、更新
 * @date: 2023/8/24 下午 4:43
 * @version: 1.0
 */
@Slf4j
@Component
public class ApplicationSummaryService {
    // 初始化ApplicationSummary
    protected ApplicationSummary initApplicationSummary(JSONObject appData,
                                                            JSONObject attemptData,
                                                            String clusterName,
                                                            String sparkHistoryServer,
                                                            String applicationId,
                                                            EnvironmentInfo environmentInfo){
        ApplicationSummary applicationSummary = new ApplicationSummary();
        // >>>>>>>>>>>> 初始化一些静态的不会更新的信息 >>>>>>>>>>>>
        applicationSummary.setClusterName(clusterName);
        applicationSummary.setSparkHistoryServer(sparkHistoryServer);
        applicationSummary.setApplicationId(applicationId);
        applicationSummary.setName(appData.getString("name"));
        applicationSummary.setUser(attemptData.getString("sparkUser"));
        applicationSummary.setStartTime(formatDateString(attemptData.getString("startTime")));
        applicationSummary.setDuration(attemptData.getString("duration"));
        applicationSummary.setCompleted(attemptData.getBoolean("completed"));
        applicationSummary.setEndTime(formatDateString(attemptData.getString("endTime")));
        applicationSummary.setLastUpdated(formatDateString(attemptData.getString("lastUpdated")));
        applicationSummary.setJavaVersion(environmentInfo.runtime.javaVersion);
        applicationSummary.setScalaVersion(environmentInfo.runtime.scalaVersion);
        applicationSummary.setSparkVersion(attemptData.getString("appSparkVersion"));
        applicationSummary.setJarsAddByUser(environmentInfo.getJarsAddByUser());
        // >>>>>>>>>>>> 打上默认的时间戳和需要初始化的标签 >>>>>>>>>>>>
        applicationSummary.setTimestamp(new Date(0));
        applicationSummary.setIsFinishInit(false);
        return applicationSummary;
    }
    // 根据时间戳判断是否需要更新ApplicationSummary
    protected ApplicationSummary updateApplicationSummary(ApplicationSummary applicationSummary,
                                                              String parentEndpoint,
                                                              JobInfo jobInfo,
                                                              StageInfo stageInfo,
                                                              ExecutorInfo executorInfo){
        if (!applicationSummary.getTimestamp().before(new Date(System.currentTimeMillis()-GUARANTEE_TIME))){
            log.info("[SPARK-INFO] " + "ApplicationSummary在保质期内，不需要更新");
            return applicationSummary;
        }
        log.info("[SPARK-INFO] " + "ApplicationSummary在保质期外，需要更新");
        // 更新链接后获取attempts信息链接有两种情况: 1. 存在attempt: 直接获取到的对象就是; 2.不存在attempt: 获取attempts标签的第一个元素
        // >>>>>>>>>>>> 获取attempts信息 >>>>>>>>>>>>
        JSONObject attemptData = null;
        try {
            JSONObject attemptData0 = httpGetAppData(parentEndpoint);
            if (attemptData0.containsKey("attemptId")){
                attemptData = attemptData0;
            }else if (attemptData0.containsKey("attempts")){
                attemptData = (JSONObject)attemptData0.getJSONArray("attempts").get(0);
            }
            else {
                throw new RuntimeException("更新Summary时获取attempt信息失败: 遇到未知情况");
            }
        } catch (Exception e) {
            log.error("[SPARK-INFO] " + "更新Summary时获取attempt信息失败: 遇到未知情况，返回旧的信息");
            return applicationSummary;
        }
        // >>>>>>>>>>>> 根据最新的attemptData、jobInfo、stageInfo、executorInfo更新状态 >>>>>>>>>>>>
        applicationSummary.setDuration(attemptData.getString("duration"));
        applicationSummary.setCompleted(attemptData.getBoolean("completed"));
        applicationSummary.setEndTime(formatDateString(attemptData.getString("endTime")));
        applicationSummary.setLastUpdated(formatDateString(attemptData.getString("lastUpdated")));
        applicationSummary.setJobCount(jobInfo.getJobCount());
        applicationSummary.setRunningJob(jobInfo.getRunningJob().size());
        applicationSummary.setSucceedJob(jobInfo.getSucceedJob().size());
        applicationSummary.setFailedJob(jobInfo.getFailedJob().size());
        applicationSummary.setUnknownJob(jobInfo.getUnknownJob().size());
        applicationSummary.setStageCount(stageInfo.getStageCount());
        applicationSummary.setActiveStage(stageInfo.getActiveStage().size());
        applicationSummary.setCompleteStage(stageInfo.getCompleteStage().size());
        applicationSummary.setFailedStage(stageInfo.getFailedStage().size());
        applicationSummary.setPendingStage(stageInfo.getPendingStage().size());
        applicationSummary.setSkippedStage(stageInfo.getSkippedStage().size());
        applicationSummary.setExecutorNums(executorInfo.getExecutorNums());
        applicationSummary.setActiveExecutor(executorInfo.getActiveExecutor().size());
        applicationSummary.setDeadExecutor(executorInfo.getDeadExecutor().size());
        // >>>>>>>>>>>> 更新时间戳及取消需要初始化标签 >>>>>>>>>>>>
        applicationSummary.setTimestamp(new Date(System.currentTimeMillis()));
        applicationSummary.setIsFinishInit(true);
        return applicationSummary;
    }
}
