package com.szubd.rsp.job;

public interface JobDubboService {
    int updateJobStatus(int jobId, String jobStatus);

    int endJob(int jobId, String jobStatus);

    int updateJobArgs(int jobId, String argName, String argValue);

    JobInfo getJobInfo(int jobId);

    int getParentId(int jobId);

    int createJob(JobInfo jobInfo);

    int endSubJob(int mvJobId, String jobStatus);

    int syncInDB(int jobId);
}
