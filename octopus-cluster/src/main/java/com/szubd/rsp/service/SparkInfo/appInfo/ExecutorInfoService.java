package com.szubd.rsp.service.SparkInfo.appInfo;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.szubd.rsp.sparkInfo.appInfo.ExecutorInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.TreeMap;

import static com.szubd.rsp.tools.MakeUpUtils.*;
import static com.szubd.rsp.tools.SparkInfoHttpUtils.httpGetExecData;

/**
 * @projectName: octopus-web
 * @package: com.szubd.rsp.service.SparkInfo.appInfo
 * @className: ExecutorInfoService
 * @author: Chandler
 * @description: ExecutorInfo的方法：初始化、更新
 * @date: 2023/8/24 上午 10:08
 * @version: 1.0
 */
@Slf4j
@Component
public class ExecutorInfoService {
    // 初始化ExecutorInfo
    public ExecutorInfo initExecutorInfo(String parentEndpoint){
        ExecutorInfo executorInfo = new ExecutorInfo();
        TreeMap<String, ExecutorInfo.ExecutorSummary> activeExecutor = new TreeMap<>();
        TreeMap<String, ExecutorInfo.ExecutorSummary> deadExecutor = new TreeMap<>();
        executorInfo.setParentEndpoint(parentEndpoint);
        // 获取Executor信息
        JSONArray execJSONArray;
        try {
            execJSONArray = httpGetExecData(executorInfo.getParentEndpoint());
            executorInfo.setExecutorNums(execJSONArray.size());
        } catch (Exception e) {
            log.error("[SPARK-INFO] " + "初始化ExecutorInfo时无法从SparkHistoryServer中获取Executor信息" + ", parentEndpoint: " +  parentEndpoint);
            executorInfo.setExecutorNums(0);
            return executorInfo;
        }
        // 构造每一个Executor的信息
        ExecutorInfo.ExecutorSummary executorSummary;
        for (Object o: execJSONArray){
            String execId = ((JSONObject) o).getString("id");
            Boolean isActive = ((JSONObject) o).getBoolean("isActive");
            try {
                executorSummary = initExecutorSummary((JSONObject) o);
            } catch (Exception e) {
                log.error("[SPARK-INFO] " + "初始化ExecutorInfo时获取ExecutorSummary失败, executorId: " + execId + ", parentEndpoint: " +  parentEndpoint);
                executorSummary = new ExecutorInfo.ExecutorSummary();
            }
            if (isActive){
                activeExecutor.put(execId, executorSummary);
            } else {
                deadExecutor.put(execId, executorSummary);
            }
        }
        // set方法更新数据
        executorInfo.setActiveExecutor(activeExecutor);
        executorInfo.setDeadExecutor(deadExecutor);
        executorInfo.setTimestamp(new Date(System.currentTimeMillis()));
        return executorInfo;
    }
    // 内部类ExecutorSummary的初始化方法
    private ExecutorInfo.ExecutorSummary initExecutorSummary(JSONObject o){
        ExecutorInfo.ExecutorSummary executorSummary = new ExecutorInfo.ExecutorSummary();
        executorSummary.setId(o.getString("id"));
        executorSummary.setHostPort(o.getString("hostPort"));
        executorSummary.setIsActive(o.getBoolean("isActive"));
        executorSummary.setRddBlocks(o.getIntValue("rddBlocks"));
        executorSummary.setMemoryUsed(makeUpString4Bit(o,"memoryUsed"));
        executorSummary.setDiskUsed(makeUpString4Bit(o,"diskUsed"));
        executorSummary.setTotalCores(o.getIntValue("totalCores"));
        executorSummary.setMaxTasks(o.getIntValue("maxTasks"));
        executorSummary.setActiveTasks(o.getIntValue("activeTasks"));
        executorSummary.setFailedTasks(o.getIntValue("failedTasks"));
        executorSummary.setCompletedTasks(o.getIntValue("completedTasks"));
        executorSummary.setTotalTasks(o.getIntValue("totalTasks"));
        executorSummary.setTotalDuration(makeUpString4Time(o,"totalDuration"));
        executorSummary.setTotalGCTime(makeUpString4Time(o,"totalGCTime"));
        executorSummary.setTotalInputBytes(makeUpString4Bit(o,"totalInputBytes"));
        executorSummary.setTotalShuffleRead(makeUpString4Bit(o,"totalShuffleRead"));
        executorSummary.setTotalShuffleWrite(makeUpString4Bit(o,"totalShuffleWrite"));
        executorSummary.setMaxMemory(makeUpString4Bit(o,"maxMemory"));
        executorSummary.setIsBlacklisted(o.getBoolean("isBlacklisted"));
        executorSummary.setAddTime(formatDateString(o.getString("addTime")));
        executorSummary.setRemoveTime(formatDateString(o.getString("removeTime")));
        executorSummary.setRemoveReason(o.getString("removeReason"));
        executorSummary.setExecutorLogs(initExecutorLogs(o.getJSONObject("executorLogs")));
        executorSummary.setMemoryMetrics(initMemoryMetrics(o.getJSONObject("memoryMetrics")));
        return executorSummary;
    }

    // 内部类ExecutorLogs的初始化方法
    private ExecutorInfo.ExecutorLogs initExecutorLogs(JSONObject o){
        ExecutorInfo.ExecutorLogs executorLogs = new ExecutorInfo.ExecutorLogs();
        try {
            executorLogs.setStderr(o.getString("stdout"));
            executorLogs.setStderr(o.getString("stderr"));
        }catch (Exception e){
            executorLogs.setStderr(null);
            executorLogs.setStderr(null);
        }
        return executorLogs;
    }

    // 内部类MemoryMetrics的初始化方法
    private ExecutorInfo.MemoryMetrics initMemoryMetrics(JSONObject o){
        ExecutorInfo.MemoryMetrics memoryMetrics = new ExecutorInfo.MemoryMetrics();
        try {
            memoryMetrics.setUsedOnHeapStorageMemory(makeUpString4Bit(o,"usedOnHeapStorageMemory"));
            memoryMetrics.setUsedOffHeapStorageMemory(makeUpString4Bit(o,"usedOffHeapStorageMemory"));
            memoryMetrics.setTotalOnHeapStorageMemory(makeUpString4Bit(o,"totalOnHeapStorageMemory"));
            memoryMetrics.setTotalOffHeapStorageMemory(makeUpString4Bit(o,"totalOffHeapStorageMemory"));
        }catch (Exception e){
            memoryMetrics.setUsedOnHeapStorageMemory(null);
            memoryMetrics.setUsedOffHeapStorageMemory(null);
            memoryMetrics.setTotalOnHeapStorageMemory(null);
            memoryMetrics.setTotalOffHeapStorageMemory(null);
        }
        return memoryMetrics;
    }

    // 根据时间戳判断是否需要更新ExecutorInfo
    public ExecutorInfo updateExecutorInfo(ExecutorInfo executorInfo){
        if (executorInfo.getTimestamp()!=null &&
                !executorInfo.getTimestamp().before(new Date(System.currentTimeMillis()-GUARANTEE_TIME))){
            log.info("[SPARK-INFO] " + "ExecutorInfo在保质期内，不需要更新");
            return executorInfo;
        }
        log.info("[SPARK-INFO] " + "ExecutorInfo在保质期外，需要更新");
        // 获取信息
        JSONArray execJSONArray;
        try {
            execJSONArray = httpGetExecData(executorInfo.getParentEndpoint());
            executorInfo.setExecutorNums(execJSONArray.size());
        } catch (Exception e) {
            log.error("[SPARK-INFO] " + "更新ExecutorInfo时无法从SparkHistoryServer中获取Executor信息" + ", parentEndpoint: " +  executorInfo.getParentEndpoint());
            return executorInfo;
        }
        // 解析信息，由于是乱序的，所以需要对其进行完全遍历
        TreeMap<String, ExecutorInfo.ExecutorSummary> activeExecutor = executorInfo.getActiveExecutor();
        TreeMap<String, ExecutorInfo.ExecutorSummary> deadExecutor = executorInfo.getDeadExecutor();
        ExecutorInfo.ExecutorSummary executorSummary;
        for (Object o: execJSONArray) {
            // 获取id、status
            String execId = ((JSONObject) o).getString("id");
            Boolean isActive = ((JSONObject) o).getBoolean("isActive");
            try {
                executorSummary = initExecutorSummary((JSONObject) o);
            } catch (Exception e) {
                log.error("[SPARK-INFO] " + "更新ExecutorInfo时获取ExecutorSummary失败, executorId: " + execId + ", parentEndpoint: " +  executorInfo.getParentEndpoint());
                executorSummary = new ExecutorInfo.ExecutorSummary();
            }
            // 对存活的进行全量更新
            if (isActive) {
                activeExecutor.put(execId, executorSummary);
            }
            else {
                // 对存活的变为不可用的进行全量更新
                if (activeExecutor.containsKey(execId)){
                    activeExecutor.remove(execId);
                    deadExecutor.put(execId, executorSummary);
                }
                // 对不可用的不更新
                else if (deadExecutor.containsKey(execId)){
                    continue;
                }
                // 保证一下完整性
                else {
//                    log.warn("executorId: " + execId + "先前不存在，将放到deadExecutor中");
                    deadExecutor.put(execId, executorSummary);
                }
            }
        }
        // set方法更新数据
        executorInfo.setActiveExecutor(activeExecutor);
        executorInfo.setDeadExecutor(deadExecutor);
        executorInfo.setTimestamp(new Date(System.currentTimeMillis()));
        return executorInfo;
    }
}
