package com.szubd.rsp.resource.bean;


import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.io.Serializable;

@Data
public class ClusterMetrics implements Serializable {
    //用对象封装起来
    private int totalNodes;
    private int activeNodes;
    private int unhealthyNodes;
    @JsonIgnore
    private int lostNodes;
    private int shutdownNodes;
    private int appsRunning;
    @JsonIgnore
    private long totalMB;
    private long totalMemoryGB; //
    private long allocatedMB;
    private String allocatedMemoryGB; //
    @JsonIgnore
    private String clusterMemoryUsage; //
    private long availableMB;
    private String availableMemoryGB; //
    private long reservedMB;
    private int totalVirtualCores;
    private int availableVirtualCores;
    private int allocatedVirtualCores;
    private int reservedVirtualCores;
    private int containersAllocated;
    private int containersReserved;
    private int containersPending;
    @JsonIgnore
    private int idChangedNum;
    private String diskUsed; //
    private String diskCapacity; //

    @Override
    public String toString() {
        return "ClusterMetrics{" +
                "totalNodes=" + totalNodes +
                ", activeNodes=" + activeNodes +
                ", unhealthyNodes=" + unhealthyNodes +
                ", lostNodes='" + lostNodes + '\'' +
                ", shutdownNodes=" + shutdownNodes +
                ", appsRunning=" + appsRunning +
                ", totalMB=" + totalMB +
                ", totalMemoryGB=" + totalMemoryGB +
                ", allocatedMB=" + allocatedMB +
                ", allocatedMemoryGB='" + allocatedMemoryGB + '\'' +
                ", clusterMemoryUsage='" + clusterMemoryUsage + '\'' +
                ", availableMB=" + availableMB +
                ", availableMemoryGB='" + availableMemoryGB + '\'' +
                ", reservedMB=" + reservedMB +
                ", totalVirtualCores=" + totalVirtualCores +
                ", availableVirtualCores=" + availableVirtualCores +
                ", allocatedVirtualCores=" + allocatedVirtualCores +
                ", reservedVirtualCores=" + reservedVirtualCores +
                ", containersAllocated=" + containersAllocated +
                ", containersReserved=" + containersReserved +
                ", containersPending=" + containersPending +
                ", idChangedNum=" + idChangedNum +
                ", diskUsed='" + diskUsed + '\'' +
                ", diskCapacity='" + diskCapacity + '\'' +
                '}';
    }
}
