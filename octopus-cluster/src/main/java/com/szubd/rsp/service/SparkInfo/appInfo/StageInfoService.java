package com.szubd.rsp.service.SparkInfo.appInfo;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.szubd.rsp.sparkInfo.appInfo.StageInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.TreeSet;

import static com.szubd.rsp.tools.MakeUpUtils.*;
import static com.szubd.rsp.tools.SparkInfoHttpUtils.httpGetStageData;

/**
 * @projectName: octopus-web
 * @package: com.szubd.rsp.service.SparkInfo.appInfo
 * @className: StageInfoService
 * @author: Chandler
 * @description: StageInfo的方法：初始化、更新
 * @date: 2023/8/24 下午 3:37
 * @version: 1.0
 */
@Slf4j
@Component
public class StageInfoService {
    // StageInfo初始化方法
    public StageInfo initStageInfo(String parentEndpoint){
        StageInfo stageInfo = new StageInfo();
        TreeSet<StageInfo.StageData> activeStage = new TreeSet<>();
        TreeSet<StageInfo.StageData> completeStage = new TreeSet<>();
        TreeSet<StageInfo.StageData> failedStage = new TreeSet<>();
        TreeSet<StageInfo.StageData> pendingStage = new TreeSet<>();
        TreeSet<StageInfo.StageData> skippedStage = new TreeSet<>();
        stageInfo.setParentEndpoint(parentEndpoint);
        // 获取信息
        JSONArray stageJSONArray;
        try {
            stageJSONArray = httpGetStageData(parentEndpoint);
            stageInfo.setStageCount(stageJSONArray.size());
        } catch (Exception e) {
            log.error("[SPARK-INFO] " + "初始化StageInfo时无法从SparkHistoryServer中获取Stage信息" + ", parentEndpoint: " +  parentEndpoint);
            stageInfo.setStageCount(0);
            return stageInfo;
        }
        // 填充数据：直接使用add方法即可，TreeSet会自动排序
        for(Object o: stageJSONArray){
            // 获取stageId
            int stageId = ((JSONObject)o).getIntValue("stageId");
            String status = ((JSONObject) o).getString("status");
            // 根据stageId分类存放stage详细信息
            // 这里因为SKIPPED的Stage是空的信息，与其他的不一样，故不先获取再分类
            try {
                // 分类
                if ("ACTIVE".equals(status)) {
                    activeStage.add(initStageData((JSONObject) o));
                } else if ("COMPLETE".equals(status)) {
                    completeStage.add(initStageData((JSONObject) o));
                    stageInfo.setCurrentNums2Update(Math.max(stageId, stageInfo.getCurrentNums2Update()));
                } else if ("PENDING".equals(status)) {
                    pendingStage.add(initStageData((JSONObject) o));
                } else if ("FAILED".equals(status)) {
                    failedStage.add(initStageData((JSONObject) o));
                    stageInfo.setCurrentNums2Update(Math.max(stageId, stageInfo.getCurrentNums2Update()));
                } else if ("SKIPPED".equals(status)) {
                    skippedStage.add(new StageInfo.StageData());
                    stageInfo.setCurrentNums2Update(Math.max(stageId, stageInfo.getCurrentNums2Update()));
                }
            } catch (Exception e) {
                log.error("[SPARK-INFO] " + "初始化StageInfo时获取StageData失败, stageId: " + stageId + ", parentEndpoint: " + parentEndpoint);
            }
        }
        // set方法更新数据
        stageInfo.setActiveStage(activeStage);
        stageInfo.setCompleteStage(completeStage);
        stageInfo.setFailedStage(failedStage);
        stageInfo.setPendingStage(pendingStage);
        stageInfo.setSkippedStage(skippedStage);
        stageInfo.setTimestamp(new Date(System.currentTimeMillis()));
        return stageInfo;
    }

    // 内部类StageData的初始化方法
    private StageInfo.StageData initStageData(JSONObject o){
        StageInfo.StageData stageData = new StageInfo.StageData();
        stageData.setStatus(o.getString("status"));
        stageData.setStageId(o.getIntValue("stageId"));
        stageData.setAttemptId(o.getIntValue("attemptId"));
        stageData.setNumTasks(o.getIntValue("numTasks"));
        stageData.setNumActiveTasks(o.getIntValue("numActiveTasks"));
        stageData.setNumCompleteTasks(o.getIntValue("numCompleteTasks"));
        stageData.setNumFailedTasks(o.getIntValue("numFailedTasks"));
        stageData.setNumKilledTasks(o.getIntValue("numKilledTasks"));
        stageData.setNumCompletedIndices(o.getIntValue("numCompletedIndices"));
        stageData.setExecutorRunTime(makeUpString4Time(o,"executorRunTime"));
        stageData.setExecutorCpuTime(makeUpString4Time(o,"executorCpuTime"));
        stageData.setSubmissionTime(formatDateString(o.getString("submissionTime")));
        stageData.setFirstTaskLaunchedTime(formatDateString(o.getString("firstTaskLaunchedTime")));
        stageData.setCompletionTime(formatDateString(o.getString("completionTime")));
        stageData.setFailureReason(o.getString("failureReason"));
        stageData.setInputBytes(makeUpString4Bit(o,"inputBytes"));
        stageData.setOutputBytes(makeUpString4Bit(o,"outputBytes"));
        stageData.setShuffleReadBytes(makeUpString4Bit(o,"shuffleReadBytes"));
        stageData.setShuffleWriteBytes(makeUpString4Bit(o,"shuffleWriteBytes"));
        stageData.setMemoryBytesSpilled(makeUpString4Bit(o,"memoryBytesSpilled"));
        stageData.setDiskBytesSpilled(makeUpString4Bit(o,"diskBytesSpilled"));
        stageData.setInputRecords(o.getLongValue("inputRecords"));
        stageData.setOutputRecords(o.getLongValue("outputRecords"));
        stageData.setShuffleReadRecords(o.getLongValue("shuffleReadRecords"));
        stageData.setShuffleWriteRecords(o.getLongValue("shuffleWriteRecords"));
        stageData.setName(o.getString("name"));
        stageData.setDetails(o.getString("details"));
        stageData.setSchedulingPool(o.getString("schedulingPool"));
        stageData.setRddIds(makeUpString4StringWithBracket(o,"rddIds"));
        return stageData;
    }

    // 根据时间戳判断是否需要更新StageInfo
    public StageInfo updateStageInfo(StageInfo stageInfo){
        if (stageInfo.getTimestamp()!=null &&
                !stageInfo.getTimestamp().before(new Date(System.currentTimeMillis()-GUARANTEE_TIME))){
            log.info("[SPARK-INFO] " + "StageInfo在保质期内，不需要更新");
            return stageInfo;
        }
        log.info("[SPARK-INFO] " + "StageInfo在保质期外，需要更新");

        // 获取信息
        JSONArray stageJSONArray;
        try {
            stageJSONArray = httpGetStageData(stageInfo.getParentEndpoint());
        } catch (Exception e) {
            log.error("[SPARK-INFO] " + "更新StageInfo时无法从SparkHistoryServer中获取Stage信息" + ", parentEndpoint: " + stageInfo.getParentEndpoint());
            return stageInfo;
        }
        // 更新信息
        stageInfo.setStageCount(stageJSONArray.size());
        // 初始化中间列表（全量更新）
        TreeSet<StageInfo.StageData> activeStage0 = new TreeSet<>();
        TreeSet<StageInfo.StageData> pendingStage0 = new TreeSet<>();
        // 获取增量更新的旧的集合
        TreeSet<StageInfo.StageData> completeStage = stageInfo.getCompleteStage();
        TreeSet<StageInfo.StageData> failedStage = stageInfo.getFailedStage();
        TreeSet<StageInfo.StageData> skippedStage = stageInfo.getSkippedStage();
        for(Object o: stageJSONArray) {
            // 获取stageId
            int stageId = ((JSONObject) o).getIntValue("stageId");
            String status = ((JSONObject) o).getString("status");
            // 获取Json信息从大到小获取，当获得Json的stageId信息小于等于当前的最大已完成stageId时不再需要更新
            if (stageId <= stageInfo.getCurrentNums2Update()){
                break;
            }
            try {
                // 分类
                if ("ACTIVE".equals(status)) {
                    activeStage0.add(initStageData((JSONObject) o));
                } else if ("COMPLETE".equals(status)) {
                    completeStage.add(initStageData((JSONObject) o));
                } else if ("PENDING".equals(status)) {
                    pendingStage0.add(initStageData((JSONObject) o));
                } else if ("FAILED".equals(status)) {
                    failedStage.add(initStageData((JSONObject) o));
                } else if ("SKIPPED".equals(status)) {
                    skippedStage.add(new StageInfo.StageData());
                }
            } catch (Exception e) {
                log.error("[SPARK-INFO] " + "更新StageInfo时获取stageData失败, stageId: " + stageId + ", parentEndpoint: " + stageInfo.getParentEndpoint());
            }
        }
        // set方法更新数据
        stageInfo.setActiveStage(activeStage0);
        stageInfo.setCompleteStage(completeStage);
        stageInfo.setFailedStage(failedStage);
        stageInfo.setPendingStage(pendingStage0);
        stageInfo.setSkippedStage(skippedStage);
        stageInfo.setTimestamp(new Date(System.currentTimeMillis()));
        return stageInfo;
    }
}
