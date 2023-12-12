package com.szubd.rsp.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class JobLogoInfo implements Serializable {
    private String userId;
    private int jobId;
    private String jobName;
    private String jobStatus;
    private String jobStartTime;
    private String jobEndTime;
    private int jobType;
    private int cores;
    private int executors;
    private int memory;
    private String runningHost;
    private HashMap<String, String> args = new HashMap<>();
    private String argsJsonString;
    private HashMap<String, String> algoArgs = new HashMap<>();
    private String algoArgsJsonString;


    public JobLogoInfo( String userId, int jobType,String jobName, String jobStatus) {
        this.jobName = jobName;
        this.jobStatus = jobStatus;
        this.jobType = jobType;
    }
    public void setExecutorArgs(int executors,int cores,int memory){
        this.executors = executors;
        this.cores = cores;
        this.memory = memory;
    }

    public void addArgs(String key, String value){
        this.args.put(key, value);
    }

    public void addAlgoArgs(String key, String value){
        this.algoArgs.put(key, value);
    }

    public void updateArgsJsonString() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        this.argsJsonString = mapper.writeValueAsString(args);
        this.algoArgsJsonString = mapper.writeValueAsString(algoArgs);
    }
}
