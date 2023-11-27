package com.szubd.rsp.sparkInfo.appInfo;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;
import java.util.TreeSet;

/**
 * @projectName: octopus-web
 * @package: com.szubd.rsp.sparkInfo.appInfo
 * @className: StageInfo
 * @author: Chandler
 * @description: 自定义的类对象，用于记录Stage的信息
 * @date: 2023/8/23 下午 4:08
 * @version: 2.0
 */
@Data
@NoArgsConstructor
public class StageInfo implements Serializable {
    private static final long serialVersionUID = 10060L;
    private int stageCount;
    private TreeSet<StageData> activeStage;
    private TreeSet<StageData> completeStage;
    private TreeSet<StageData> failedStage;
    private TreeSet<StageData> pendingStage;
    private TreeSet<StageData> skippedStage;
    private String parentEndpoint;
    private int currentNums2Update;
    private Date timestamp;

    /**
     * 记录单个Stage的信息
     * */
    @Data
    @NoArgsConstructor
    public static class StageData implements Serializable, Comparable<StageData>{
        private static final long serialVersionUID = 10061L;
        public String status;
        public int stageId;
        public int attemptId;
        public int numTasks;
        public int numActiveTasks;
        public int numCompleteTasks;
        public int numFailedTasks;
        public int numKilledTasks;
        public int numCompletedIndices;
        public String executorRunTime;
        public String executorCpuTime;
        public String submissionTime;
        public String firstTaskLaunchedTime;
        public String completionTime;
        public String failureReason;
        public String inputBytes;
        public long inputRecords;
        public String outputBytes;
        public long outputRecords;
        public String shuffleReadBytes;
        public long shuffleReadRecords;
        public String shuffleWriteBytes;
        public long shuffleWriteRecords;
        public String memoryBytesSpilled;
        public String diskBytesSpilled;
        public String name;
        public String details;
        public String schedulingPool;
        public String rddIds;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            StageData stageData = (StageData) o;
            return stageId == stageData.stageId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(stageId);
        }

        @Override
        public int compareTo(StageData o) {
            // 升序
//            return Integer.compare(this.stageId, o.stageId);
            // 降序
            return Integer.compare(o.stageId, this.stageId);
        }
    }
}

