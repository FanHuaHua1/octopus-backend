package com.szubd.rsp.sparkInfo;

import com.szubd.rsp.sparkInfo.appInfo.*;
import com.szubd.rsp.sparkInfo.clusterInfo.AppIdList;
import com.szubd.rsp.sparkInfo.clusterInfo.AppIdTree;

import java.util.List;

/**
 * 约定的接口，用于远程调用
 * 关于SparkInfo的相关接口：获取信息
 * */
public interface SparkInfoDubboService {
    AppIdTree getAppIdTree();

    AppIdList getAppIdList(String clusterName);

    ApplicationSummary getAppSummary(String clusterName, String applicationId);

    JobInfo getJobInfo(String clusterName, String applicationId);

    StageInfo getStageInfo(String clusterName, String applicationId);

    EnvironmentInfo getEnvironmentInfo(String clusterName, String applicationId);

    ExecutorInfo getExecutorInfo(String clusterName, String applicationId);
}
