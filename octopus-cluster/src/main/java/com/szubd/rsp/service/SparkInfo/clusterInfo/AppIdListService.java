package com.szubd.rsp.service.SparkInfo.clusterInfo;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.szubd.rsp.sparkInfo.clusterInfo.AppIdList;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;

import static com.szubd.rsp.tools.MakeUpUtils.GUARANTEE_TIME;
import static com.szubd.rsp.tools.SparkInfoHttpUtils.httpGetAppList4SparkServerLimit15;
import static com.szubd.rsp.tools.SparkInfoHttpUtils.httpGetRunningAppList4YarnServer;

/**
 * @projectName: octopus-web
 * @package: com.szubd.rsp.service.SparkInfo.clusterInfo
 * @className: AppIdListService
 * @author: Chandler
 * @description: AppIdList的初始化和更新
 * @date: 2023/8/24 下午 8:09
 * @version: 1.0
 */
@Slf4j
@Component
public class AppIdListService {
    // 初始化AppIdList
    public AppIdList initAppIdList(String yarnResourceManagerServer, String sparkHistoryServer) {
        // 初始化成员变量
        AppIdList appIdList = new AppIdList();
        appIdList.setSparkHistoryServer(sparkHistoryServer);
        appIdList.setYarnResourceManagerServer(yarnResourceManagerServer);
        appIdList.setBlackApplicationIdSet(new HashSet<>());
        appIdList.setRunningApp(new ArrayList<>());
        appIdList.setCompletedApp(new ArrayList<>());
        appIdList.setTimestamp(new Date(0));
        // 初始化任务列表
        try {
            updateAppIdList(appIdList);
        }catch (Exception e){
            appIdList.setRunningApp(new ArrayList<>());
            appIdList.setCompletedApp(new ArrayList<>());
            log.error("[SPARK-INFO] " + "初始化任务列表失败, sparkHistoryServer: " + sparkHistoryServer + ", yarnResourceManagerServer: " + yarnResourceManagerServer);
        }
        return appIdList;
    }

    // 封装一层，保证获取的是最新的数据
    public AppIdList getUpdateAppIdList(AppIdList appIdList){
        return updateAppIdList(appIdList);
    }

    // 根据时间戳判断是否需要更新AppIdList
    private AppIdList updateAppIdList(AppIdList appIdList){
        if (appIdList.getTimestamp()!=null &&
                !appIdList.getTimestamp().before(new Date(System.currentTimeMillis()-GUARANTEE_TIME))){
            log.info("[SPARK-INFO] " + "更新AppIdList时，信息在保质期内，不需要更新");
            return appIdList;
        }
        log.info("[SPARK-INFO] " + "更新AppIdList时，信息在保质期外，需要更新");
        // 覆盖更新，注意需要重新set
        ArrayList<String>runningApp0 = new ArrayList<>();
        JSONObject runningAppJson = httpGetRunningAppList4YarnServer(appIdList.getYarnResourceManagerServer());
        if (runningAppJson.getJSONObject("apps").isEmpty()){
            log.warn("[SPARK-INFO] " + "获取RunningAppList为空, yarnResourceManagerServer: " + appIdList.getYarnResourceManagerServer());
        }
        else {
            // 解析Json
            for (Object o : runningAppJson.getJSONObject("apps").getJSONArray("app")) {
                String applicationType = ((JSONObject) o).getString("applicationType");
                if ("SPARK".equals(applicationType)) {
                    String id = ((JSONObject) o).getString("id");
                    // 去除local的任务
                    if (id.startsWith("local")){
                        continue;
                    }
                    // 去除黑名单中的任务
                    else if (appIdList.getBlackApplicationIdSet().contains(id)){
                        log.warn("[SPARK-INFO] " + "id: " + id + "已经添加到黑名单，不再进行获取");
                    }else{
                        runningApp0.add(id);
                    }
                }
            }
            // 排序
            runningApp0.sort(Comparator.reverseOrder());
            log.info("[SPARK-INFO] " + "获取RunningAppList成功, yarnResourceManagerServer: " + appIdList.getYarnResourceManagerServer());
        }
        // 覆盖更新
        ArrayList<String> completedApp0 = new ArrayList<>();
        JSONArray completedAppArray = httpGetAppList4SparkServerLimit15(appIdList.getSparkHistoryServer());
        if (completedAppArray.isEmpty()){
            log.warn("[SPARK-INFO] " + "获取CompletedAppList为空, sparkHistoryServer: " + appIdList.getSparkHistoryServer());
        }
        else {
            // 解析Json
            for (Object o : completedAppArray) {
                String id = ((JSONObject) o).getString("id");
                Boolean isCompleted = ((JSONObject)((JSONArray)((JSONObject) o).get("attempts")).get(0)).getBoolean("completed");
                if (appIdList.getBlackApplicationIdSet().contains(id)){
                    log.warn("[SPARK-INFO] " + "id: " + id + "已经添加到黑名单，不再进行获取");
                    continue;
                }
                // 这里不获取未完成的任务, 去除local的任务
                else if (id.startsWith("local") || !isCompleted){
                    continue;
                }
                else {
                    completedApp0.add(id);
                }
            }
            completedApp0.sort(Comparator.reverseOrder());
            log.info("[SPARK-INFO] " + "获取CompletedAppList成功, sparkHistoryServer: " + appIdList.getSparkHistoryServer());
        }
        // set方法更新变量
        appIdList.setRunningApp(runningApp0);
        appIdList.setCompletedApp(completedApp0);
        appIdList.setTimestamp(new Date(System.currentTimeMillis()));
        return appIdList;
    }
}
