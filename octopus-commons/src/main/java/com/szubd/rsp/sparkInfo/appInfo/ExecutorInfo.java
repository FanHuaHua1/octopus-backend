package com.szubd.rsp.sparkInfo.appInfo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.*;

/**
 * @projectName: octopus-web
 * @package: com.szubd.rsp.sparkInfo.appInfo
 * @className: ExecutorInfo
 * @author: Chandler
 * @description: 自定义的类对象，用于记录所有Executor信息
 * @date: 2023/8/23 下午 4:00
 * @version: 2.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ExecutorInfo implements Serializable {
    private static final long serialVersionUID = 10040L;
    private TreeMap<String, ExecutorSummary> activeExecutor;
    private TreeMap<String, ExecutorSummary> deadExecutor;
    private Integer executorNums;
    private String parentEndpoint;
    private Date timestamp;

    @Data
    @NoArgsConstructor
    public static class ExecutorSummary implements Serializable, Comparable<ExecutorSummary>{
        private static final long serialVersionUID = 10041L;
        public String id;
        public String hostPort;
        public Boolean isActive;
        public int rddBlocks;
        public String memoryUsed;
        public String diskUsed;
        public int totalCores;
        public int maxTasks;
        public int activeTasks;
        public int failedTasks;
        public int completedTasks;
        public int totalTasks;
        public String totalDuration;
        public String totalGCTime;
        public String totalInputBytes;
        public String totalShuffleRead;
        public String totalShuffleWrite;
        public Boolean isBlacklisted;
        public String maxMemory;
        public String addTime;
        public String removeTime;
        public String removeReason;
        public ExecutorLogs executorLogs;
        public MemoryMetrics memoryMetrics;

        @Override
        public int compareTo(ExecutorSummary o) {
            // diver排在前面
            if ("diver".equals(this.id)){
                return -Integer.MAX_VALUE;
            }else if("diver".equals(o.id)){
                return Integer.MAX_VALUE;
            }
            try {
                return Integer.compare(Integer.parseInt(this.id),Integer.parseInt(o.id));
            } catch (NumberFormatException e){
                // 默认比较器：使用字符顺序比较
                return this.id.compareTo(o.id);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ExecutorSummary executorSummary = (ExecutorSummary) o;
            return Objects.equals(this.id, executorSummary.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.id);
        }
    }

    @Data
    @NoArgsConstructor
    public static class MemoryMetrics implements Serializable{
        private static final long serialVersionUID = 10042L;
        public String usedOnHeapStorageMemory;
        public String usedOffHeapStorageMemory;
        public String totalOnHeapStorageMemory;
        public String totalOffHeapStorageMemory;
    }

    @Data
    @NoArgsConstructor
    public static class ExecutorLogs implements Serializable{
        private static final long serialVersionUID = 10043L;
        public String stdout;
        public String stderr;
    }
    // 前端需要数组格式，覆盖原get方法
    public ExecutorSummary[] getActiveExecutorList() {
        ArrayList<ExecutorSummary> list = new ArrayList<>(activeExecutor.values());
        list.sort(executorComparator);
        return list.toArray(new ExecutorSummary[0]);
    }

    public ExecutorSummary[] getDeadExecutorList() {
        ArrayList<ExecutorSummary> list = new ArrayList<>(deadExecutor.values());
        list.sort(executorComparator);
        return list.toArray(new ExecutorSummary[0]);
    }
    @JsonIgnore
    public TreeMap<String, ExecutorSummary> getActiveExecutor(){
        return this.activeExecutor;
    }
    @JsonIgnore
    public TreeMap<String, ExecutorSummary> getDeadExecutor(){
        return this.activeExecutor;
    }


    public static Comparator<ExecutorSummary> executorComparator = (o1, o2) -> {
        // driver排在前面
        if ("driver".equals(o1.id)){
            return -Integer.MAX_VALUE;
        }else if("driver".equals(o2.id)){
            return Integer.MAX_VALUE;
        }
        try {
            return Integer.compare(Integer.parseInt(o1.id),Integer.parseInt(o2.id));
        } catch (NumberFormatException e){
            // 默认比较器：使用字符顺序比较
            return o1.id.compareTo(o2.id);
        }
    };
}
