package com.szubd.rsp.service.job;

import com.szubd.rsp.job.JobInfo;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

import java.util.List;

@Mapper
@Repository
public interface JobInfoMapper {
    List<JobInfo> listAllJobInfo();
    int insertJobInfo(JobInfo jobInfo);
    int updateJobInfo(JobInfo jobInfo);

    int getAvaliableJobId();
}
