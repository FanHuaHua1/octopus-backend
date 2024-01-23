package com.szubd.rsp.service.SparkInfo.appInfo;

import com.alibaba.fastjson.JSONObject;
import com.szubd.rsp.sparkInfo.appInfo.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.szubd.rsp.tools.SparkInfoHttpUtils.httpGetAppData;

/**
 * @projectName: octopus-web
 * @package: com.szubd.rsp.service.SparkInfo.appInfo
 * @className: ApplicationInfoService
 * @author: Chandler
 * @description: ApplicationInfo的方法：初始化、更新
 * @date: 2023/8/24 下午 4:36
 * @version: 1.0
 */
@Slf4j
@Component
public class ApplicationInfoService {
    @Autowired
    private ApplicationSummaryService applicationSummaryService;
    @Autowired
    private EnvironmentInfoService environmentInfoService;
    @Autowired
    private ExecutorInfoService executorInfoService;
    @Autowired
    private JobInfoService jobInfoService;
    @Autowired
    private StageInfoService stageInfoService;

    // 初始化ApplicationInfo，会预先生成一部分信息：部分ApplicationSummary、全量不变的EnvironmentInfo，其余为null
    public ApplicationInfo initApplicationInfo(String sparkHistoryServer, String applicationId, String clusterName){
        ApplicationInfo applicationInfo = new ApplicationInfo();
        // >>>>>>>>>>>>>>>>> 拼接请求链接 >>>>>>>>>>>>>>>>>
        // 拼接基础请求链接
        applicationInfo.setApplicationId(applicationId);
        applicationInfo.setParentEndpoint("http://" + sparkHistoryServer + "/api/v1/applications/" + applicationId);
        // 获取基础信息和attempts信息
        JSONObject appData = httpGetAppData(applicationInfo.getParentEndpoint());
        // 添加检测判断，如果attempts项多于1会警告
        if (appData.containsKey("attempts") && appData.getJSONArray("attempts").size() > 1){
            log.warn("[SPARK-INFO] " + "attempts次数大于1，只获取最新的index的attempts信息");
        }
        // 只会获取最新的一个attempts的信息，我们设定的规则：attempts只重试一次。在其他场景可能会出现bug
        JSONObject attemptData = (JSONObject)appData.getJSONArray("attempts").get(0);
        // 判断是否存在attemptId，存在则需要对parentEndpoint进行拼接attemptId，
        if (attemptData.containsKey("attemptId")){
            String attemptId = attemptData.getString("attemptId");
            applicationInfo.setParentEndpoint(applicationInfo.getParentEndpoint() + "/" + attemptData.getString("attemptId"));
            log.warn("[SPARK-INFO] " + "使用attemptId: " + attemptId + ", 更新parentEndpoint: " + applicationInfo.getParentEndpoint());
        }
        // >>>>>>>>>>>>>>>>> 初始化jobs、stages、environment、executors、summary信息 >>>>>>>>>>>>>>>>>
        applicationInfo.setJobInfo(null);
        applicationInfo.setStageInfo(null);
        applicationInfo.setExecutorInfo(null);
        applicationInfo.setEnvironmentInfo(null);
        applicationInfo.setEnvironmentInfo(getUpdateEnvironmentInfo(applicationInfo));
        // 预先生成一部分不会变的摘要信息
        try {
            applicationInfo.setApplicationSummary(applicationSummaryService.initApplicationSummary(
                    appData,
                    attemptData,
                    clusterName,
                    sparkHistoryServer,
                    applicationId,
                    applicationInfo.getEnvironmentInfo()));
            log.info("[SPARK-INFO] " + "预生成applicationSummary成功, AppId: " + applicationId);
        } catch (Exception e) {
            log.error("[SPARK-INFO] " + "预生成applicationSummary失败, AppId: " + applicationId);
        }
        return applicationInfo;
    }

    /**
     * 获取最新的ApplicationSummary、JobInfo、StageInfo、ExecutorInfo、EnvironmentInfo的方法
     * 根据ApplicationSummary的completed（是否是已经完成的任务）判断是否需要更新：
     * --true：说明是已经完成的任务，不需要再更新，直接返回旧值；
     * --false：说明是正在运行的任务，调用具体的更新方法update。
     * 其中ApplicationSummary更新时会全量更新相关对象，可以保证ApplicationSummary的completed变化时其他值也是最新的
     * */
    public ApplicationSummary getUpdateApplicationSummary(ApplicationInfo applicationInfo) {
        // ApplicationInfoSummary在生成ApplicationInfo时会预先生成，故一般情况下不会存在不存在的情况
        if (applicationInfo.getApplicationSummary() == null){
            log.error("[SPARK-INFO] " + "ApplicationSummary未初始化, AppId: " + applicationInfo.getApplicationId());
            return null;
        }
        // 需要初始化或未完成的任务：需要更新
        if (!applicationInfo.getApplicationSummary().getIsFinishInit() || !applicationInfo.getApplicationSummary().getCompleted()){
            ApplicationSummary applicationSummary0 = applicationSummaryService.updateApplicationSummary(
                    applicationInfo.getApplicationSummary(),
                    applicationInfo.getParentEndpoint(),
                    getUpdateJobInfo(applicationInfo),
                    getUpdateStageInfo(applicationInfo),
                    getUpdateExecutorInfo(applicationInfo));
            applicationInfo.setApplicationSummary(applicationSummary0);
        }
        return applicationInfo.getApplicationSummary();
    }

    public JobInfo getUpdateJobInfo(ApplicationInfo applicationInfo) {
        // 未构造：初始化JobInfo
        if (applicationInfo.getJobInfo() == null){
            try {
                applicationInfo.setJobInfo(jobInfoService.initJobInfo(applicationInfo.getParentEndpoint()));
                log.info("[SPARK-INFO] " + "初始化jobInfo成功, AppId: " + applicationInfo.getApplicationId());
            } catch (Exception e) {
                log.error("[SPARK-INFO] " + "初始化jobInfo失败, AppId: " + applicationInfo.getApplicationId());
            }
        }
        // 未完成的任务：需要更新
        else if (!applicationInfo.getApplicationSummary().getCompleted()){
            applicationInfo.setJobInfo(jobInfoService.updateJobInfo(applicationInfo.getJobInfo()));
        }
        return applicationInfo.getJobInfo();
    }

    public StageInfo getUpdateStageInfo(ApplicationInfo applicationInfo) {
        // 未构造：初始化StageInfo
        if (applicationInfo.getStageInfo() == null){
            try {
                applicationInfo.setStageInfo(stageInfoService.initStageInfo(applicationInfo.getParentEndpoint()));
                log.info("[SPARK-INFO] " + "初始化stageInfo成功, AppId: " + applicationInfo.getApplicationId());
            } catch (Exception e) {
                log.error("[SPARK-INFO] " + "初始化stageInfo失败, AppId: " + applicationInfo.getApplicationId());
            }
        }
        // 未完成的任务：需要更新
        else if (!applicationInfo.getApplicationSummary().getCompleted()){
            applicationInfo.setStageInfo(stageInfoService.updateStageInfo(applicationInfo.getStageInfo()));
        }
        return applicationInfo.getStageInfo();
    }

    public ExecutorInfo getUpdateExecutorInfo(ApplicationInfo applicationInfo){
        // 未构造：初始化ExecutorInfo
        if (applicationInfo.getExecutorInfo() == null){
            try {
                applicationInfo.setExecutorInfo(executorInfoService.initExecutorInfo(applicationInfo.getParentEndpoint()));
                log.info("[SPARK-INFO] " + "初始化executorInfo, AppId: " + applicationInfo.getApplicationId());
            } catch (Exception e) {
                log.error("[SPARK-INFO] " + "初始化executorInfo失败, AppId: " + applicationInfo.getApplicationId());
            }
        }
        // 未完成的任务：需要更新
        else if (!applicationInfo.getApplicationSummary().getCompleted()){
            applicationInfo.setExecutorInfo(executorInfoService.updateExecutorInfo(applicationInfo.getExecutorInfo()));
        }
        return applicationInfo.getExecutorInfo();
    }

    public EnvironmentInfo getUpdateEnvironmentInfo(ApplicationInfo applicationInfo) {
        // 未构造：初始化EnvironmentInfo
        if (applicationInfo.getEnvironmentInfo() == null){
            try {
                applicationInfo.setEnvironmentInfo(environmentInfoService.initEnvironmentInfo(applicationInfo.getParentEndpoint()));
                log.info("[SPARK-INFO] " + "初始化environmentInfo成功, AppId: " + applicationInfo.getApplicationId());
            } catch (Exception e) {
                log.error("[SPARK-INFO] " + "初始化environmentInfo失败, AppId: " + applicationInfo.getApplicationId());
            }
        }
        // EnvironmentInfo信息不需要更新
        return applicationInfo.getEnvironmentInfo();
    }
}
