package com.szubd.rsp.resource.bean;


import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.io.Serializable;

@Data
public class ClusterInfo implements Serializable {
    private Long id;
    private Long startedOn;
    private String state;
    private String haState;
    @JsonIgnore
    private String rmStateStoreName;
    private String resourceManagerVersion;
    @JsonIgnore
    private String resourceManagerBuildVersion;
    @JsonIgnore
    private String resourceManagerVersionBuiltOn;
    private String hadoopVersion;
    @JsonIgnore
    private String hadoopBuildVersion;
    @JsonIgnore
    private String hadoopVersionBuiltOn;
    @JsonIgnore
    private String haZooKeeperConnectionState;
    //CDH集群名称
    private String cdhClusterName;
    //CDH版本
    private String cdhVersion;
    private String clusterUrl;
    private String allHostUrl;
    //集群运行状态
    private String clusterRunningStatus;

    @Override
    public String toString() {
        return "ClusterInfo{" +
                "id=" + id +
                ", startedOn=" + startedOn +
                ", state='" + state + '\'' +
                ", haState='" + haState + '\'' +
                ", rmStateStoreName='" + rmStateStoreName + '\'' +
                ", resourceManagerVersion='" + resourceManagerVersion + '\'' +
                ", resourceManagerBuildVersion='" + resourceManagerBuildVersion + '\'' +
                ", resourceManagerVersionBuiltOn='" + resourceManagerVersionBuiltOn + '\'' +
                ", hadoopVersion='" + hadoopVersion + '\'' +
                ", hadoopBuildVersion='" + hadoopBuildVersion + '\'' +
                ", hadoopVersionBuiltOn='" + hadoopVersionBuiltOn + '\'' +
                ", haZooKeeperConnectionState='" + haZooKeeperConnectionState + '\'' +
                ", cdhClusterName='" + cdhClusterName + '\'' +
                ", cdhVersion='" + cdhVersion + '\'' +
                ", clusterUrl='" + clusterUrl + '\'' +
                ", allHostUrl='" + allHostUrl + '\'' +
                ", clusterRunningStatus='" + clusterRunningStatus + '\'' +
                '}';
    }
}
