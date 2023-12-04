package com.szubd.rsp.service.job;

import com.szubd.rsp.job.JobLogoInfo;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

import java.util.List;

@Mapper
@Repository
public interface JobLogoInfoMapper {
    List<JobLogoInfo> listAllJobInfo();

    int insertJobInfo(JobLogoInfo info);
    int updateJobInfo(JobLogoInfo info);

    int getAvaliableJobId();
}
