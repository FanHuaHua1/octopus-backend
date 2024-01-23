package com.szubd.rsp.sparkInfo.appInfo;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;

/**
 * @projectName: octopus-web
 * @package: com.szubd.rsp.sparkInfo.appInfo
 * @className: ApplicationSummary
 * @author: Chandler
 * @description: 自定义的类对象，用于记录Application的简要信息，依赖于其他模块
 * @date: 2023/8/23 下午 3:52
 * @version: 2.0
 */
@NoArgsConstructor
@Data
public class ApplicationSummary implements Serializable {
    private static final long serialVersionUID = 10020L;
    private String applicationId;
    private String sparkHistoryServer;
    private String clusterName;
    private String name;
    private String user;
    private Boolean completed;
    private String startTime;
    private String endTime;
    private String lastUpdated;
    private String duration;
    private int jobCount;
    private int runningJob;
    private int succeedJob;
    private int failedJob;
    private int unknownJob;
    private int stageCount;
    private int activeStage;
    private int completeStage;
    private int failedStage;
    private int pendingStage;
    private int skippedStage;
    private int executorNums;
    private int activeExecutor;
    private int deadExecutor;
    private String javaVersion;
    private String scalaVersion;
    private String sparkVersion;
    private ArrayList<String> jarsAddByUser;
    private Date timestamp;
    private Boolean isFinishInit;
}
