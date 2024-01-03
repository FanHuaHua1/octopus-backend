package com.szubd.rsp.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class JobInfo implements Serializable {
    private int jobId;
    private String jobName;
    private String jobStatus;
    private String jobStartTime;
    private String jobEndTime;
    private int jobType;
    private int subJobNum;
    private int parentJobId;
    private String runningHost;
    private HashMap<String, String> args = new HashMap<>();
    private String argsJsonString;
    private HashMap<String, String> algoArgs = new HashMap<>();
    private String algoArgsJsonString;


    public JobInfo(int jobType, String jobName, String jobStatus, String runningHost, int parentJobId) {
        this(jobType, jobName, jobStatus, runningHost);
        this.setParentJobId(parentJobId);
    }
    public JobInfo(int jobType, String jobName, String jobStatus) throws UnknownHostException {
        this(jobType, jobName, jobStatus, InetAddress.getLocalHost().getHostAddress());
    }

    public JobInfo(int jobType, String jobName, String jobStatus,int parentJobId) throws UnknownHostException {
        this(jobType, jobName, jobStatus, InetAddress.getLocalHost().getHostAddress(),parentJobId);
    }
    public JobInfo(int jobType, String jobName, String jobStatus, String runningHost) {
        this.setJobType(jobType);
        this.setJobName(jobName);
        this.setJobStatus(jobStatus);
        this.setRunningHost(runningHost);
    }

    public void addArgs(String key, String value){
        this.args.put(key, value);
    }

    public void addMultiArgs(String... kvs){
        if(kvs.length == 0 || kvs.length % 2 != 0){
            for(int i = 0; i < kvs.length; i += 2){
                this.args.put(kvs[i], kvs[i + 1]);
            }
        }

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
