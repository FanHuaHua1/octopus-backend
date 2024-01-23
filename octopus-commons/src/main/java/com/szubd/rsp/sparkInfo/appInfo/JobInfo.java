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
 * @className: JobInfo
 * @author: Chandler
 * @description: 自定义的类对象，用于记录所有Job的信息
 * @date: 2023/8/23 下午 4:04
 * @version: 2.0
 */
@NoArgsConstructor
@Data
public class JobInfo implements Serializable{
    private static final long serialVersionUID = 10050L;
    private int jobCount;
    private TreeSet<JobData> runningJob;
    private TreeSet<JobData> succeedJob;
    private TreeSet<JobData> failedJob;
    private TreeSet<JobData> unknownJob;
    private String parentEndpoint;
    private int currentNums2Update;
    private Date timestamp;

    /**
     * 记录单个Job的信息
     * */
    @NoArgsConstructor
    @Data
    public static class JobData implements Serializable, Comparable<JobData>{
        private static final long serialVersionUID = 10051L;
        public int jobId;
        public String name;
        public String submissionTime;
        public String completionTime;
        public String stageIds;
        public String status;
        public int numTasks;
        public int numActiveTasks;
        public int numCompletedTasks;
        public int numSkippedTasks;
        public int numFailedTasks;
        public int numKilledTasks;
        public int numCompletedIndices;
        public int numActiveStages;
        public int numCompletedStages;
        public int numSkippedStages;
        public int numFailedStages;
        public int numStages;
        public String killedTasksSummary;

        @Override
        public int compareTo(JobData o) {
            // 降序
            return Integer.compare(o.jobId, this.jobId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            JobData jobData = (JobData) o;
            return jobId == jobData.jobId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId);
        }
    }
}

