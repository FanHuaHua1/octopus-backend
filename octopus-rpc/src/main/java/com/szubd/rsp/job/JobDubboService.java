package com.szubd.rsp.job;

public interface JobDubboService {
    int updateJobStatus(int jobId, String jobStatus);

    int endJob(int jobId, String jobStatus);

    int updateJobArgs(int jobId, String argName, String argValue);

    int updateMultiJobArgs(int jobId, String... kvs);

    JobInfo getJobInfo(int jobId);

    int getParentId(int jobId);

    int createJob(JobInfo jobInfo);

    int endSubJob(int mvJobId, String jobStatus);

    int syncInDB(int jobId);

    void createOrUpdateJobCountDown(int jobId, int count);

    void reduceJobCountDown(int jobId);

    int getJobCountDown(int jobId);

}
