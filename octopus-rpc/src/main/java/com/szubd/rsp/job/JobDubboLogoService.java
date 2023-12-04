package com.szubd.rsp.job;


public interface JobDubboLogoService {


    int updateJobStatus(int jobId, String jobStatus);

    int endJob(int jobId, String jobStatus);

    int updateJobArgs(int jobId, String argName, String argValue);

    JobLogoInfo getJobInfo(int jobId);


    int createJob(JobLogoInfo jobInfo);


    int syncInDB(int jobId);


}
