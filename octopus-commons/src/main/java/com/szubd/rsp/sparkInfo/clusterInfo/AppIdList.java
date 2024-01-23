package com.szubd.rsp.sparkInfo.clusterInfo;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;

/**
 * @projectName: octopus-web
 * @package: com.szubd.rsp.sparkInfo.clusterInfo
 * @className: AppIdList
 * @author: Chandler
 * @description: 从yarn集群中的ResourceManager获取正在运行的spark ApplicationId，从SparkHistoryServer获取结束的spark
 *               ApplicationId，分别存放在两个ArrayList中
 * @date: 2023/8/23 下午 4:11
 * @version: 2.0
 */
@Data
@NoArgsConstructor
public class AppIdList implements Serializable {
    private static final long serialVersionUID = 10004L;
    private ArrayList<String> runningApp;
    private ArrayList<String> completedApp;
    private String yarnResourceManagerServer;
    private String sparkHistoryServer;
    private HashSet<String> blackApplicationIdSet;
    private Date timestamp;
}

