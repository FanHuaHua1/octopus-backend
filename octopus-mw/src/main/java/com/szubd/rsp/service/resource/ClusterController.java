package com.szubd.rsp.service.resource;

import com.alibaba.nacos.api.naming.pojo.Instance;
import com.szubd.rsp.http.Result;
import com.szubd.rsp.http.ResultResponse;
import com.szubd.rsp.resource.ClusterResourceService;
import com.szubd.rsp.resource.bean.ClusterInfo;
import com.szubd.rsp.resource.bean.ClusterMetrics;
import com.szubd.rsp.resource.bean.ClusterNode;
import com.szubd.rsp.service.node.NacosService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static com.szubd.rsp.http.ResultCode.CONNECT_IS_INVALID;
import static com.szubd.rsp.http.ResultCode.PARAMS_IS_INVALID;
import static com.szubd.rsp.tools.DubboUtils.getServiceRef;

// 简单业务逻辑全写Controller层
@Slf4j
@CrossOrigin
@RestController
@RequestMapping("/resource")
public class ClusterController {

    @Autowired
    private NacosService nacosService;
    //不能只初始化一次的
    public static List<String> ips;

    //@ModelAttribute注解会在每个带有@RequestMapping注解的方法执行之前执行
    @ModelAttribute
    public void executeBeforeRequest() {
        // 在请求到达处理方法之前执行的操作
        log.info("[SPARK-INFO] Executing before request operation");
        ips=getAliveServiceIps();
    }

    //心跳检测
    @Scheduled(fixedRate = 20000)
    public void checkAlive() {
        // 配置文件转化的集合拉过来 一定要注意程序的执行顺序 避免空指针异常
        getAliveServiceIps();
    }

    @GetMapping("/ClustersInfo")
    public Result getClustersInfo() {
        log.info("[SPARK-INFO] ClustersInfo获取信息");
        HashMap<String, ClusterInfo> clustersInfoMap = new HashMap<>();
        for (String ip : ips) {
            ClusterResourceService clusterService = getClusterService(ip);
            ClusterInfo clusterInfo = clusterService.getClusterInfo();
            if(null==clusterInfo){
                return ResultResponse.failure(CONNECT_IS_INVALID);
            }
            clustersInfoMap.put(ip, clusterInfo);
        }
        return ResultResponse.success(clustersInfoMap);
    }

    @GetMapping("/ClustersInfo/{RmHostIp}")
    public Result getClusterInfo(@PathVariable String RmHostIp) {
        if(!ips.contains(RmHostIp)){
            return ResultResponse.failure(PARAMS_IS_INVALID);
        }
        log.info("[SPARK-INFO] ClusterInfo获取信息:"+RmHostIp);
        ClusterInfo clusterInfo = getClusterService(RmHostIp).getClusterInfo();
        if(null==clusterInfo){
            return ResultResponse.failure(CONNECT_IS_INVALID);
        }
        return ResultResponse.success(clusterInfo);
    }

    @GetMapping("/ClustersMetricsInfo")
    public Result getClustersMetricsInfo() {
    log.info("[SPARK-INFO] ClustersMetricsInfo获取信息");
    HashMap<String, ClusterMetrics> clustersMetricsMap = new HashMap<>();
    for (String ip : ips) {
        ClusterResourceService clusterService = getClusterService(ip);
        ClusterMetrics clusterMetricsInfo = clusterService.getClusterMetricsInfo();
        if(null==clusterMetricsInfo){
            return ResultResponse.failure(CONNECT_IS_INVALID);
        }
        clustersMetricsMap.put(ip, clusterMetricsInfo);
     }
    return ResultResponse.success(clustersMetricsMap);
    }

    @GetMapping("/ClustersMetricsInfo/{RmHostIp}")
    public Result getClusterMetricsInfo(@PathVariable String RmHostIp) {
        if(!ips.contains(RmHostIp)){
            return ResultResponse.failure(PARAMS_IS_INVALID);
        }
        log.info("[SPARK-INFO] ClusterMetricsInfo获取信息:"+RmHostIp);
        ClusterMetrics clusterMetricsInfo = getClusterService(RmHostIp).getClusterMetricsInfo();
        if(null==clusterMetricsInfo){
            return ResultResponse.failure(CONNECT_IS_INVALID);
        }
        return ResultResponse.success(clusterMetricsInfo);
    }

    @GetMapping("/ClustersNodesInfo")
    public Result getClustersNodesInfo(){
        log.info("[SPARK-INFO] ClustersNodesInfo获取信息");
        HashMap<String, List<ClusterNode>> ClusterNodesMap = new HashMap<>();
        for (String ip : ips) {
            ClusterResourceService clusterService = getClusterService(ip);
            List<ClusterNode> clusterNodesInfo = clusterService.getClusterNodesInfo();
            if(null==clusterNodesInfo){
                return ResultResponse.failure(CONNECT_IS_INVALID);
            }
            ClusterNodesMap.put(ip, clusterNodesInfo);
        }
        return ResultResponse.success(ClusterNodesMap);
    }

    @GetMapping("/ClustersNodesInfo/{RmHostIp}")
    public Result getClusterNodesInfo(@PathVariable String RmHostIp){
        if(!ips.contains(RmHostIp)){
            return ResultResponse.failure(PARAMS_IS_INVALID);
        }
        log.info("[SPARK-INFO] ClusterNodesInfo获取信息:"+RmHostIp);
        List<ClusterNode> clusterNodesInfo = getClusterService(RmHostIp).getClusterNodesInfo();
        if(null==clusterNodesInfo){
            return ResultResponse.failure(CONNECT_IS_INVALID);
        }
        return ResultResponse.success(clusterNodesInfo);
    }

    private ClusterResourceService getClusterService(String RmHostIp){
        log.info("[SPARK-INFO] dubbo://"+RmHostIp+":20880/com.szubd.rsp.resource.ClusterResourceService");
        return getServiceRef(RmHostIp, "com.szubd.rsp.resource.ClusterResourceService");
    }

    private List<String> getAliveServiceIps(){
        try {
            // 获取集群列表
            List<Instance> instances = nacosService.listAliveCluster();
            // 获取ip集合：查询当前存活节点
            List<String> ips = instances.stream().map(Instance::getIp).collect(Collectors.toList());
            //log.info("[SPARK-INFO] listAliveCluster:"+ips);
            return ips;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}