package com.szubd.rsp.service.sparkInfo;

import com.alibaba.nacos.api.naming.pojo.Instance;
import com.szubd.rsp.node.NodeInfo;
import com.szubd.rsp.node.NodeInfoService;
import com.szubd.rsp.service.node.NacosService;
import com.szubd.rsp.sparkInfo.SparkInfoDubboService;
import com.szubd.rsp.sparkInfo.appInfo.*;
import com.szubd.rsp.sparkInfo.clusterInfo.AppIdList;
import com.szubd.rsp.sparkInfo.clusterInfo.AppIdTree;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

import static com.szubd.rsp.tools.DubboUtils.getServiceRef;

/**
 * @projectName: octopus-web
 * @package: com.szubd.rsp.service.sparkInfo
 * @className: SparkInfoService
 * @author: Chandler
 * @description: 中间件获取远程集群服务，并通过NacosService、NodeInfoService对存活的节点进行监控，获取对应的服务调用，实现先前
 *               ClusterTable的分发管理功能。由于是自动进行监控的，先前的添加删除方法被弃用。
 *               提供的方法直接封装好可以对接接口SparkInfoDubboService
 * @date: 2023/8/23 下午 3:44
 * @version: 2.0
 */
@Component
@Slf4j
public class SparkInfoService {

    // 获取服务节点信息
    @Autowired
    private NodeInfoService nodeInfoService;
    // 获取存活节点
    @Autowired
    private NacosService nacosService;

    private HashMap<String, SparkInfoDubboService> node2service = new HashMap<>();

    public List<AppIdTree> getAppIdTree() {
        // 获取最新的服务
        updateService();
        List<AppIdTree> res = new ArrayList<>();
        for(SparkInfoDubboService service: node2service.values()){
            res.add(service.getAppIdTree());
        }
        return res;
    }

    public AppIdList getAppIdList(String clusterName) {
        return this.getService(clusterName).getAppIdList(clusterName);
    }

    public ApplicationSummary getAppSummary(String clusterName, String applicationId) {

        return this.getService(clusterName).getAppSummary(clusterName, applicationId);
    }

    public JobInfo getJobInfo(String clusterName, String applicationId) {
        return this.getService(clusterName).getJobInfo(clusterName, applicationId);
    }

    public StageInfo getStageInfo(String clusterName, String applicationId) {
        return this.getService(clusterName).getStageInfo(clusterName, applicationId);
    }

    public EnvironmentInfo getEnvironmentInfo(String clusterName, String applicationId) {
        return this.getService(clusterName).getEnvironmentInfo(clusterName, applicationId);
    }

    public ExecutorInfo getExecutorInfo(String clusterName, String applicationId) {
        return this.getService(clusterName).getExecutorInfo(clusterName, applicationId);
    }

    private SparkInfoDubboService getService(String clusterName){
        if (!node2service.containsKey(clusterName)){
            log.warn("[SPARK-INFO] ClusterName服务未链接, clusterName: " + clusterName);
            log.info("[SPARK-INFO] 重新链接服务");
            updateService();
        }
        return node2service.get(clusterName);
    }

    private void updateService() {
        log.info("[SPARK-INFO] 更新可用的服务");
        try {
            // 获取集群列表
            List<Instance> instances = nacosService.listAliveCluster();
            // 获取ip集合：查询当前存活节点
            Set<String> aliveIpSet = instances.stream().map(Instance::getIp).collect(Collectors.toSet());
            HashMap<String, SparkInfoDubboService> node2service0 = new HashMap<>();
            for(String ip : aliveIpSet){
                // 通过ip获取节点信息：查询表nodeInfo
                NodeInfo nodeInfo = nodeInfoService.queryForNodeInfoByIp(ip);
                // 获取集群名称和服务
                String clusterName = nodeInfo.getClusterName();
                SparkInfoDubboService sparkInfoDubboService = getServiceRef(ip, "com.szubd.rsp.sparkInfo.SparkInfoDubboService");
                // 直接添加，覆盖旧的引用
                node2service0.put(clusterName, sparkInfoDubboService);
            }
            node2service = node2service0;
        } catch (Exception e) {
            log.error("[SPARK-INFO] 更新集群信息引用失败");
            log.error(String.valueOf(e));
        }
    }
}
