package com.szubd.rsp.service.SparkInfo.clusterInfo;

import com.szubd.rsp.constants.ClusterConfigConstant;
import com.szubd.rsp.service.SparkInfo.appInfo.ApplicationInfoService;
import com.szubd.rsp.sparkInfo.appInfo.ApplicationInfo;
import com.szubd.rsp.sparkInfo.clusterInfo.AppIdList;
import com.szubd.rsp.sparkInfo.clusterInfo.AppIdTree;
import com.szubd.rsp.sparkInfo.clusterInfo.ClusterData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;

/**
 * @projectName: octopus-web
 * @package: com.szubd.rsp.service.SparkInfo.clusterInfo
 * @className: ClusterDataService
 * @author: Chandler
 * @description: ClusterData代理整个集群的信息，不再使用ClusterTable进行管理，相关逻辑在中间件中实现。
 *               提供ClusterData的方法：初始化、获取本集群的AppIdTree和AppIdList、获取本集群的任务ApplicationInfo
 * @date: 2023/8/24 下午 8:06
 * @version: 2.0
 */
@Slf4j
@Component
public class ClusterDataService {
    private static final ClusterData clusterData = new ClusterData();

    @Autowired
    private AppIdListService appIdListService;
    @Autowired
    private ApplicationInfoService applicationInfoService;
    @Autowired
    private ClusterConfigConstant clusterConfigConstant;

    // 初始化ClusterData，生成本集群的AppIdList和空的ApplicationInfo池
    @PostConstruct
    private ClusterData initClusterData(){
        log.info("[SPARK-INFO] " + "Spark任务监控启动, 根据配置文件初始化监视服务器的信息，生成ClusterData");
        clusterData.setClusterName(clusterConfigConstant.clusterName);
        clusterData.setYarnResourceManager(clusterConfigConstant.yarnResourceManager);
        clusterData.setSparkHistoryServer(clusterConfigConstant.sparkHistoryServer);
        clusterData.setApplicationInfoHashMap(new HashMap<>());
        try {
            clusterData.setAppIdList(appIdListService.initAppIdList(clusterConfigConstant.yarnResourceManager, clusterConfigConstant.sparkHistoryServer));
            log.info("[SPARK-INFO] " + "获取AppIdList成功, clusterName: " + clusterConfigConstant.clusterName);
        }catch (Exception e){
            clusterData.setAppIdList(new AppIdList());
            log.error("[SPARK-INFO] " + "获取AppIdList失败, clusterName: " + clusterConfigConstant.clusterName);
        }
        return clusterData;
    }
    // 获取ClusterData中的ApplicationInfo池中的ApplicationInfo
    public ApplicationInfo getApplicationInfo(String applicationId){
        // 判断是否存在，不存在则新建
        if (!clusterData.containsApplicationId(applicationId)){
            clusterData.addApplicationInfo(applicationId,
                    applicationInfoService.initApplicationInfo(clusterData.getSparkHistoryServer(), applicationId, clusterData.getClusterName()));
        }
        return clusterData.getApplicationInfo(applicationId);
    }
    // 获取本集群的AppIdTree
    public AppIdTree getAppTree(){
        String clusterName = clusterData.getClusterName();
        AppIdList appIdList = appIdListService.getUpdateAppIdList(clusterData.getAppIdList());
        // 构造服务器AppTree
        AppIdTree clusterAppTree = new AppIdTree(clusterName);
        // 构造runningAppTree
        AppIdTree runningAppTree = new AppIdTree("RunningApp");
        for (String runningAppId: appIdList.getRunningApp()){
            runningAppTree.addChildren(new AppIdTree(runningAppId));
        }
        clusterAppTree.addChildren(runningAppTree);
        // 构造completedAppTree
        AppIdTree completedAppTree = new AppIdTree("CompletedApp");
        for (String completedAppId: appIdList.getCompletedApp()){
            completedAppTree.addChildren(new AppIdTree(completedAppId));
        }
        clusterAppTree.addChildren(completedAppTree);
        return clusterAppTree;
    }
    // 获取本集群的AppIdList
    public AppIdList getAppIdList(){
        return clusterData.getAppIdList();
    }
}
