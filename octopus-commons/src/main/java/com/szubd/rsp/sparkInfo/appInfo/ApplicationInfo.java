package com.szubd.rsp.sparkInfo.appInfo;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @projectName: octopus-web
 * @package: com.szubd.rsp.sparkInfo.appInfo
 * @className: ApplicationInfo
 * @author: Chandler
 * @description: 自定义的类对象，用于记录Application的信息，会存储全部的信息，包括：jobInfo、stageInfo、executorInfo、
 *               environmentInfo以及依赖于他们的摘要信息ApplicationSummary
 * @date: 2023/8/23 下午 3:50
 * @version: 2.0
 */
@Data
@NoArgsConstructor
public class ApplicationInfo implements Serializable {
    private static final long serialVersionUID = 10010L;
    private String applicationId;
    // parentEndpoint格式: http://[sparkHistoryServer]/api/v1/applications/[application-id]，末尾没有"/"
    private String parentEndpoint;
    private JobInfo jobInfo;
    private StageInfo stageInfo;
    private ExecutorInfo executorInfo;
    private EnvironmentInfo environmentInfo;
    private ApplicationSummary applicationSummary;
}
