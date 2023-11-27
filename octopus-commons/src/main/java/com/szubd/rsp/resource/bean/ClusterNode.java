package com.szubd.rsp.resource.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
@Data
public class ClusterNode implements Serializable,Comparable<ClusterNode>{
    private String nodeHostName;
    private String ip;
    private int cpuCoreNum;
    private String totalMemoryGB;
    private int threadNum;
    private String nodeState;
    private int numContainers;
    private int availMemoryMB;
    //可使用的内存总值
    private String availMemoryGB;
    private int usedMemoryMB;
    //在使用的内存总值
    private String usedMemoryGB;
    //总共分配给hadoop调度的内存总值=可使用的内存总值+在使用的内存总值
    private String totalAllocatedMemoryGB;
    @JsonIgnore
    private String nodeMemoryUsage;
    private int availableVirtualCores;
    private int usedVirtualCores;
    private long nodePhysicalMemoryMB;
    private String nodePhysicalMemoryGB;
    private long nodeVirtualMemoryMB;
    private String nodeCPUUsage;
    private long aggregatedContainersPhysicalMemoryMB;
    private long aggregatedContainersVirtualMemoryMB;
    private String containersCPUUsage;
    private String lastHeart;
    //保存相关进程服务内容
    private List<String> services = new ArrayList<>();
    private String rack;
    private String dfsUsed;
    private String dfsRemain;
    private String dfsUsedPercent;

    // 通过排序与hdfsAPI的dataNode节点数据进行匹配
    @Override
    public int compareTo(ClusterNode otherClusterNode) {
        return this.nodeHostName.compareTo(otherClusterNode.nodeHostName);
    }
}
