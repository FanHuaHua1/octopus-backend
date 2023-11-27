package com.szubd.rsp.service.resource.dao;

import com.szubd.rsp.resource.bean.ClusterInfo;
import com.szubd.rsp.resource.bean.ClusterMetrics;
import com.szubd.rsp.resource.bean.ClusterNode;
import com.cloudera.api.ApiRootResource;
import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.DataView;
import com.cloudera.api.model.ApiCluster;
import com.cloudera.api.model.ApiClusterList;
import com.cloudera.api.model.ApiHost;
import com.cloudera.api.model.ApiRoleRef;
import com.cloudera.api.v10.HostsResourceV10;
import com.cloudera.api.v30.ClustersResourceV30;
import com.cloudera.api.v30.RootResourceV30;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.szubd.rsp.resource.dao.ClusterInfoResponse;
import com.szubd.rsp.resource.dao.ClusterMetricsResponse;
import com.szubd.rsp.service.resource.bean.Cluster;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;


@Slf4j
@Component
public class ApiDao {
    @Autowired
    private Cluster cluster;
    private ApiClusterList apiClusterList; // 都是每个集群的
    private RootResourceV30 apiRoot;
    private FsStatus diskStatus;
    private ClusterInfo clusterInfo;
    private ClusterMetrics clusterMetricsInfo;
    private List<ClusterNode> clusterNodesInfo;
    //小数位数格式化
    private static final DecimalFormat df = new DecimalFormat("#0.00");
    // 创建RestTemplate实例
    private final RestTemplate restTemplate = new RestTemplate();

    public void initOrUpdate(){
        // 初始化这些集合也可以采取clear的形式 不用反复new
            // System.out.println(cluster);
            ApiRootResource apiRootResource = new ClouderaManagerClientBuilder().withHost(cluster.getCdhApiPath())
                    .withPort(7180).withUsernamePassword("admin", cluster.getCdhApiPassword())
                    .build();
           this.apiRoot = apiRootResource.getRootV30();
            try {
                //log.info("[SPARK-INFO] 连接CM");
                //真正抛出异常的代码块
                ClustersResourceV30 clustersResourcev30 = apiRoot.getClustersResource();
                this.apiClusterList=clustersResourcev30.readClusters(DataView.FULL);
                //log.info("[SPARK-INFO] 连接CM成功");
            } catch (Exception e) {
                log.error("输入信息有误，请重新输入!");
            }

        //========================CMAPI========================//
        //for(Cluster cluster:clusters){
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, "true");
        String nnUrl = "hdfs://" + cluster.getNnHostIp() + ":8020";
        DistributedFileSystem fs = getFs(nnUrl,conf);
        DFSClient dfsClient = fs.getClient();
        DatanodeInfo[] dataNodes;
        try {
            diskStatus = dfsClient.getDiskStatus();
            dataNodes = dfsClient.getNamenode().getDatanodeReport(HdfsConstants.DatanodeReportType.ALL);
        } catch (Exception e) {
            log.warn("高可用HA切换为备用节点IP:"+cluster.getNnHostBackupIp());
            // nnIp = properties.getProperty("nnHostBackupIp");
            nnUrl = "hdfs://" + cluster.getNnHostBackupIp() + ":8020";
            fs = getFs(nnUrl,conf);
            dfsClient = fs.getClient();
            try {
                diskStatus = dfsClient.getDiskStatus();
                dataNodes = dfsClient.getNamenode().getDatanodeReport(HdfsConstants.DatanodeReportType.ALL);
            } catch (IOException ex) {
                log.error("NNBackUpIp提供出错");
                throw new RuntimeException(ex);
            }
        }

        //========================HDFSAPI========================//
        //=======================================================//
        // clusterInfo= restTemplate.getForObject("http://172.31.238.102:8088/ws/v1/cluster", ClusterInfo.class); 不对
        String url = "http://"+cluster.getRmHostIp()+ ":8088/ws/v1/cluster";
            ClusterInfoResponse clusterInfoResponse = restTemplate.getForObject(url, ClusterInfoResponse.class);
            ClusterInfo clusterInfo = clusterInfoResponse != null ? clusterInfoResponse.getClusterInfo() : null;
            assert clusterInfo!=null;
            // System.out.println(clusterInfo);
            // 补充CloudreaManagerAPI信息
            // CDH管理下的集群个数  apiroot数组貌似没用
            List<ApiCluster> apiClusters = apiClusterList.getClusters();
            ApiCluster apiCluster = apiClusters.get(0);
            String cdhClusterName = apiCluster.getName();
            if (clusterInfo != null) {
                clusterInfo.setCdhClusterName(cdhClusterName);
                clusterInfo.setClusterUrl(apiCluster.getClusterUrl());
                clusterInfo.setCdhVersion(apiCluster.getFullVersion());
                clusterInfo.setAllHostUrl(apiCluster.getHostsUrl());
                clusterInfo.setClusterRunningStatus(apiCluster.getEntityStatus().toString());
            }
            //log.info("[SPARK-INFO] 获取集群{}资源信息中",cdhClusterName);
            this.clusterInfo=clusterInfo;
            //=======================================================//
            url = "http://"+cluster.getRmHostIp()+ ":8088/ws/v1/cluster/metrics";
            ClusterMetricsResponse clusterMetricsResponse = restTemplate.getForObject(url, ClusterMetricsResponse.class);
            ClusterMetrics clusterMetricsInfo = clusterMetricsResponse != null ? clusterMetricsResponse.getClusterMetrics() : null;
            if (clusterMetricsInfo != null) {
               clusterMetricsInfo.setTotalMemoryGB(clusterMetricsInfo.getTotalMB()/1024);
               clusterMetricsInfo.setAllocatedMemoryGB(df.format(clusterMetricsInfo.getAllocatedMB()/1024.0));
               clusterMetricsInfo.setClusterMemoryUsage(df.format((float)clusterMetricsInfo.getAllocatedMB()/clusterMetricsInfo.getTotalMB()));
               clusterMetricsInfo.setAvailableMemoryGB(df.format(clusterMetricsInfo.getAvailableMB()/1024.0));
               clusterMetricsInfo.setDiskCapacity(df.format(diskStatus.getCapacity()/(1024.0*1024)/(1024.0*1024.0)));
               clusterMetricsInfo.setDiskUsed(df.format(diskStatus.getUsed()/(1024.0*1024)/1024.0/1024.0));
            }
            this.clusterMetricsInfo=clusterMetricsInfo;
            //=======================================================//
            url = "http://"+cluster.getRmHostIp()+ ":8088/ws/v1/cluster/nodes";
            //+个库处理 不直接转变为对象
            //NodesInfoResponse nodesInfoResponse=restTemplate.getForObject(url,NodesInfoResponse.class);
            String json = restTemplate.getForObject(url, String.class);
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode=null;
            try {
                rootNode = mapper.readTree(json);
            } catch (JsonProcessingException e) {
                log.error("readTree读取失败");
                throw new RuntimeException(e);
            }
            //得到集群所有节点的JSON响应正文内容
            JsonNode nodeArray = rootNode.path("nodes").path("node");
            List<ClusterNode> clusterNodeList = new LinkedList<>();
            for (JsonNode node : nodeArray) {
                ClusterNode clusterNode = new ClusterNode();
                clusterNode.setNodeHostName(node.path("nodeHostName").asText());
                //=================================================================================
                //CDH资源管理信息获取
                HostsResourceV10 hostsResourceV10 = apiRoot.getHostsResource();
                ApiHost cmApinode = hostsResourceV10.readHost(node.path("nodeHostName").asText());
                clusterNode.setIp(cmApinode.getIpAddress());
                clusterNode.setCpuCoreNum(cmApinode.getNumPhysicalCores().intValue());
                clusterNode.setThreadNum(cmApinode.getNumCores().intValue());
                clusterNode.setRack(cmApinode.getRackId());
                clusterNode.setLastHeart(String.valueOf(cmApinode.getLastHeartbeat()));
                clusterNode.setTotalMemoryGB(df.format(cmApinode.getTotalPhysMemBytes() / (1024.0 * 1024.0 * 1024.0)));
                //===================================================================================
                clusterNode.setNodeState(node.path("state").asText());
                clusterNode.setNumContainers(node.path("numContainers").asInt());
                //=====================================内存==========================================
                int usedMemoryMB = node.path("usedMemoryMB").asInt();
                clusterNode.setUsedMemoryMB(usedMemoryMB);
                clusterNode.setUsedMemoryGB(df.format(usedMemoryMB/1024.0));
                int availMemoryMB = node.path("availMemoryMB").asInt();
                clusterNode.setAvailMemoryMB(availMemoryMB);
                clusterNode.setAvailMemoryGB(df.format(availMemoryMB/1024.0));
                clusterNode.setNodeMemoryUsage(df.format((float)usedMemoryMB/(usedMemoryMB+availMemoryMB)));
                clusterNode.setTotalAllocatedMemoryGB(df.format(usedMemoryMB/1024.0+availMemoryMB/1024.0));
                clusterNode.setUsedVirtualCores(node.path("usedVirtualCores").asInt());
                clusterNode.setAvailableVirtualCores(node.path("availableVirtualCores").asInt());
                JsonNode resourceUtilizationNode = node.path("resourceUtilization");
                long nodePhysicalMemoryMB = resourceUtilizationNode.path("nodePhysicalMemoryMB").asLong();
                clusterNode.setNodePhysicalMemoryMB(nodePhysicalMemoryMB);
                clusterNode.setNodePhysicalMemoryGB(df.format(nodePhysicalMemoryMB/1024.0));
                clusterNode.setNodeVirtualMemoryMB(resourceUtilizationNode.path("nodeVirtualMemoryMB").asLong());
                clusterNode.setNodeCPUUsage(df.format(resourceUtilizationNode.path("nodeCPUUsage").asDouble()));
                clusterNode.setAggregatedContainersPhysicalMemoryMB(resourceUtilizationNode.path("aggregatedContainersPhysicalMemoryMB").asLong());
                clusterNode.setAggregatedContainersVirtualMemoryMB(resourceUtilizationNode.path("aggregatedContainersVirtualMemoryMB").asLong());
                clusterNode.setContainersCPUUsage(df.format(resourceUtilizationNode.path("containersCPUUsage").asDouble()));
                List<String> services = new ArrayList<>();
                for (ApiRoleRef apiRoleRef : cmApinode.getRoleRefs()) {
                    //System.out.println(apiRoleRef.getRoleName());
                    //字符串切割只保留substring第二个-前面的字符串内容
                    String roleName = apiRoleRef.getRoleName();
                    int position = roleName.indexOf("-", roleName.indexOf("-") + 1);
                    String substring = roleName.substring(0, position);
                    services.add(substring);
                }
                clusterNode.setServices(services);
                clusterNodeList.add(clusterNode);
            }
                Collections.sort(clusterNodeList);
            for (int i=0;i<clusterNodeList.size();i++){
                ClusterNode clusterNode = clusterNodeList.get(i);
                clusterNode.setDfsUsed(df.format(dataNodes[i].getDfsUsed()/1024.0/1024.0/1024.0/1024.0));
                clusterNode.setDfsRemain(df.format(dataNodes[i].getRemaining()/1024.0/1024.0/1024.0/1024.0));
                clusterNode.setDfsUsedPercent(df.format(dataNodes[i].getDfsUsedPercent()));
            }
                this.clusterNodesInfo=clusterNodeList;
                // log.info(clusterNodesInfo.toString());
    }

    // 反复使用的抽离出方法
    private DistributedFileSystem getFs(String nnUrl, Configuration conf){
        DistributedFileSystem fs = null;
        try {
            fs = (DistributedFileSystem) FileSystem.get(URI.create(nnUrl), conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return fs;
    }

    public RootResourceV30 getApiRoot(){
            return apiRoot;
    }
    public ApiClusterList getApiClusterList() {
        return apiClusterList;
    }
    public FsStatus getDiskStatus() {
        return diskStatus;
    }
    public ClusterInfo getClusterInfo() {
        return clusterInfo;
    }
    public ClusterMetrics getClusterMetricsInfo() {
        return clusterMetricsInfo;
    }
    public List<ClusterNode> getClusterNodesInfo() {
        return clusterNodesInfo;
    }
}
