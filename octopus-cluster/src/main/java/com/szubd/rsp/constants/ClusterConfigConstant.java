package com.szubd.rsp.constants;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @projectName: octopus-web
 * @package: com.szubd.rsp.constants
 * @className: ClusterConfigConstant
 * @author: Chandler
 * @description: Cluster的名称、地址配置
 * @date: 2023/8/24 下午 7:07
 * @version: 1.0
 */

@Component
public class ClusterConfigConstant {

    @Value("${spark-info.clusterConfig.clusterName}")
    public String clusterName;
    @Value("${spark-info.clusterConfig.yarnResourceManager}")
    public String yarnResourceManager;
    @Value("${spark-info.clusterConfig.sparkHistoryServer}")
    public String sparkHistoryServer;
}

