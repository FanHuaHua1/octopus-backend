package com.szubd.rsp.service.sparkInfo;

import com.szubd.rsp.http.Result;
import com.szubd.rsp.http.ResultResponse;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

/**
 * @projectName: octopus-web
 * @package: com.szubd.rsp.service.sparkInfo
 * @className: SparkInfoController
 * @author: Chandler
 * @description: 中间件提供的访问接口
 * @date: 2023/8/25 下午 4:14
 * @version: 1.0
 */
@Slf4j
@Controller
@RequestMapping("/sparkInfo")
public class SparkInfoController {

    @Autowired
    private SparkInfoService sparkInfoService;
    /**
     * 获取所有的appId信息，以Tree的形式返回
     * */
    @ResponseBody
    @GetMapping("/getAppIdTree")
    public Result handleGetAppIdTree(){
        try {
            log.info("[SPARK-INFO] " + "获取ApplicationIdTree");
            return ResultResponse.success(sparkInfoService.getAppIdTree());
        } catch (Exception e) {
            log.error("[SPARK-INFO] " + "获取ApplicationIdTree失败");
            return ResultResponse.failure(null);
        }
    }

    /**
     * 获取某个集群的appId，以数组的形式分别返回运行完的和正在运行的appId
     * @param clusterName 约定的集群名字，如：Adam
     * */
    @ResponseBody
    @GetMapping("/{clusterName}")
    public Result handleGetAppIdList(@PathVariable String clusterName){
        try {
            log.info("[SPARK-INFO] " + "获取ApplicationList, clusterName: " + clusterName);
            return ResultResponse.success(sparkInfoService.getAppIdList(clusterName));
        } catch (Exception e) {
            log.error("[SPARK-INFO] " + "获取ApplicationList失败, clusterName: " + clusterName);
            return ResultResponse.failure(null);
        }
    }

    /**
     * 获取ApplicationSummary，通过摘要信息可以访问更加详细的信息
     * @param clusterName 约定的集群名字，如：Adam
     * @param applicationId applicationId，如：application_1678350187685_0276
     * */
    @ResponseBody
    @GetMapping("/{clusterName}/{applicationId}")
    public Result handleGetAppSummary(@PathVariable String clusterName, @PathVariable String applicationId) {
        try {
            log.info("[SPARK-INFO] " + "获取ApplicationSummary, clusterName: " + clusterName + ", applicationId:" + applicationId);
            return ResultResponse.success(sparkInfoService.getAppSummary(clusterName, applicationId));
        } catch (Exception e) {
            log.error("[SPARK-INFO] " + "获取ApplicationSummary失败, clusterName: " + clusterName + ", applicationId:" + applicationId);
            return ResultResponse.failure(null);
        }
    }

    /**
     * 获取JobData（全量，分类）
     * @param clusterName 约定的集群名字，如：Adam
     * @param applicationId applicationId，如：application_1678350187685_0276
     * */
    @ResponseBody
    @GetMapping("/{clusterName}/{applicationId}/jobData")

    public Result handleGetJobInfo(@PathVariable String clusterName, @PathVariable String applicationId) {
        try {
            log.info("[SPARK-INFO] " + "获取JobInfo, clusterName: " + clusterName + ", applicationId:" + applicationId);
            return ResultResponse.success(sparkInfoService.getJobInfo(clusterName, applicationId));
        } catch (Exception e) {
            log.error("[SPARK-INFO] " + "获取JobInfo失败, clusterName: " + clusterName + ", applicationId:" + applicationId);
            return ResultResponse.failure(null);
        }
    }

    /**
     * 获取StageData（全量，分类）
     * @param clusterName 约定的集群名字，如：Adam
     * @param applicationId applicationId，如：application_1678350187685_0276
     * */
    @ResponseBody
    @GetMapping("/{clusterName}/{applicationId}/stageData")
    public Result handleGetStageData(@PathVariable String clusterName, @PathVariable String applicationId) {
        try {
            log.info("[SPARK-INFO] " + "获取StageInfo, clusterName: " + clusterName + ", applicationId:" + applicationId);
            return ResultResponse.success(sparkInfoService.getStageInfo(clusterName, applicationId));
        } catch (Exception e) {
            log.error("[SPARK-INFO] " + "获取StageInfo失败, clusterName: " + clusterName + ", applicationId:" + applicationId);
            return ResultResponse.failure(null);
        }
    }

    /**
     * 获取EnvironmentInfo
     * @param clusterName   约定的集群名字，如：Adam
     * @param applicationId applicationId，如：application_1678350187685_0276
     */
    @ResponseBody
    @GetMapping("/{clusterName}/{applicationId}/environment")
    public Result handleGetEnvironmentInfo(@PathVariable String clusterName, @PathVariable String applicationId) {
        try {
            log.info("[SPARK-INFO] " + "获取EnvironmentInfo, clusterName: " + clusterName + ", applicationId:" + applicationId);
            return ResultResponse.success(sparkInfoService.getEnvironmentInfo(clusterName, applicationId));
        } catch (Exception e) {
            log.error("[SPARK-INFO] " + "获取EnvironmentInfo失败, clusterName: " + clusterName + ", applicationId:" + applicationId);
            return ResultResponse.failure(null);
        }
    }

    /**
     * 获取ExecutorSummary（全量，分类）
     * @param clusterName 约定的集群名字，如：Adam
     * @param applicationId applicationId，如：application_1678350187685_0276
     * */
    @ResponseBody
    @GetMapping("/{clusterName}/{applicationId}/executorData")
    public Result handleGetExecutorData(@PathVariable String clusterName, @PathVariable String applicationId) {
        try {
            log.info("[SPARK-INFO] " + "获取ExecutorInfo, clusterName: " + clusterName + ", applicationId:" + applicationId);
            return ResultResponse.success(sparkInfoService.getExecutorInfo(clusterName, applicationId));
        } catch (Exception e) {
            log.error("[SPARK-INFO] " + "获取ExecutorInfo失败, clusterName: " + clusterName + ", applicationId:" + applicationId);
            return ResultResponse.failure(null);
        }
    }
}
