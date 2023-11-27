package com.szubd.rsp.sparkInfo.clusterInfo;

import com.szubd.rsp.sparkInfo.appInfo.ApplicationInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;

/**
 * @projectName: octopus-web
 * @package: com.szubd.rsp.sparkInfo.clusterInfo
 * @className: ClusterData
 * @author: Chandler
 * @description: 存储cluster的基础信息和获取的Spark Application的信息
 * @date: 2023/8/23 下午 4:17
 * @version: 2.0
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class ClusterData implements Serializable {
    private static final long serialVersionUID = 10002L;
    private String clusterName;
    private String sparkHistoryServer;
    private String yarnResourceManager;
    private AppIdList appIdList;
    private HashMap<String, ApplicationInfo> applicationInfoHashMap;

    // 判断是否存在对应id的ApplicationInfo
    public Boolean containsApplicationId(String applicationId){
        return this.applicationInfoHashMap.containsKey(applicationId);
    }

    // 添加ApplicationInfo
    public void addApplicationInfo(String applicationId, ApplicationInfo applicationInfo){
        this.applicationInfoHashMap.put(applicationId, applicationInfo);
    }

    // 获取ApplicationInfo，若不存在则返回null，需要进行检测，若为null需要进行初始化而不是直接返回
    public ApplicationInfo getApplicationInfo(String applicationId){
        return this.applicationInfoHashMap.getOrDefault(applicationId, null);
    }

}
