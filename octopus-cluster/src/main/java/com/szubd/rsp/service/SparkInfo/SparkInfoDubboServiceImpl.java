package com.szubd.rsp.service.SparkInfo;

import com.szubd.rsp.service.SparkInfo.appInfo.ApplicationInfoService;
import com.szubd.rsp.service.SparkInfo.clusterInfo.AppIdListService;
import com.szubd.rsp.service.SparkInfo.clusterInfo.ClusterDataService;
import com.szubd.rsp.sparkInfo.SparkInfoDubboService;
import com.szubd.rsp.sparkInfo.appInfo.*;
import com.szubd.rsp.sparkInfo.clusterInfo.AppIdList;
import com.szubd.rsp.sparkInfo.clusterInfo.AppIdTree;
import lombok.extern.slf4j.Slf4j;
import org.apache.dubbo.config.annotation.DubboService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
/**
 * @projectName: octopus-web
 * @package: com.szubd.rsp.service.SparkInfo
 * @className: SparkInfoDubboServiceImpl
 * @author: Chandler
 * @description: 整合封装方法，实现接口定义的方法，直接对接接口interface和REST Controller。移除添加集群和删除集群方法。
 * @date: 2023/8/24 下午 9:00
 * @version: 1.0
 */
// TODO: 添加集群名字判断，不一样的话得提出报错！
@Component
@DubboService
@Slf4j
public class SparkInfoDubboServiceImpl implements SparkInfoDubboService {

    @Autowired
    private ClusterDataService clusterDataService;
    @Autowired
    private ApplicationInfoService applicationInfoService;
    @Autowired
    private AppIdListService appIdListService;

    @Override
    public AppIdTree getAppIdTree() {
        try {
            log.info("[SPARK-INFO] " + "获取ApplicationIdTree");
            return clusterDataService.getAppTree();
        }catch (Exception e){
            log.error("[SPARK-INFO] " + "获取ApplicationIdTree失败");
            return null;
        }
    }

    @Override
    public AppIdList getAppIdList(String clusterName) {
        try {
            log.info("[SPARK-INFO] " + "获取ApplicationList, clusterName: " + clusterName);
            return appIdListService.getUpdateAppIdList(clusterDataService.getAppIdList());
        }catch (Exception e){
            log.error("[SPARK-INFO] " + "获取ApplicationList失败, clusterName: " + clusterName);
            return null;
        }
    }

    @Override
    public ApplicationSummary getAppSummary(String clusterName, String applicationId) {
        try {
            log.info("[SPARK-INFO] " + "获取ApplicationSummary, clusterName: " + clusterName + ", applicationId:" + applicationId);
            return applicationInfoService.getUpdateApplicationSummary(clusterDataService.getApplicationInfo(applicationId));
        }catch (Exception e){
            log.error("[SPARK-INFO] " + "获取ApplicationSummary失败, clusterName: " + clusterName + ", applicationId:" + applicationId);
            return null;
        }
    }

    @Override
    public JobInfo getJobInfo(String clusterName, String applicationId) {
        try {
            log.info("[SPARK-INFO] " + "获取JobInfo, clusterName: " + clusterName + ", applicationId:" + applicationId);
            return applicationInfoService.getUpdateJobInfo(clusterDataService.getApplicationInfo(applicationId));
        }catch (Exception e){
            log.error("[SPARK-INFO] " + "获取JobInfo失败, clusterName: " + clusterName + ", applicationId:" + applicationId);
            return null;
        }
    }

    @Override
    public StageInfo getStageInfo(String clusterName, String applicationId) {
        try {
            log.info("[SPARK-INFO] " + "获取StageInfo, clusterName: " + clusterName + ", applicationId:" + applicationId);
            return applicationInfoService.getUpdateStageInfo(clusterDataService.getApplicationInfo(applicationId));
        }catch (Exception e){
            log.error("[SPARK-INFO] " + "获取StageInfo失败, clusterName: " + clusterName + ", applicationId:" + applicationId);
            return null;
        }
    }

    @Override
    public EnvironmentInfo getEnvironmentInfo(String clusterName, String applicationId) {
        try {
            log.info("[SPARK-INFO] " + "获取EnvironmentInfo, clusterName: " + clusterName + ", applicationId:" + applicationId);
            return applicationInfoService.getUpdateEnvironmentInfo(clusterDataService.getApplicationInfo(applicationId));
        }catch (Exception e){
            log.error("[SPARK-INFO] " + "获取EnvironmentInfo失败, clusterName: " + clusterName + ", applicationId:" + applicationId);
            return null;
        }
    }

    @Override
    public ExecutorInfo getExecutorInfo(String clusterName, String applicationId) {
        try {
            log.info("[SPARK-INFO] " + "获取ExecutorInfo, clusterName: " + clusterName + ", applicationId:" + applicationId);
            return applicationInfoService.getUpdateExecutorInfo(clusterDataService.getApplicationInfo(applicationId));
        }catch (Exception e){
            log.error("[SPARK-INFO] " + "获取ExecutorInfo失败, clusterName: " + clusterName + ", applicationId:" + applicationId);
            return null;
        }
    }
}
