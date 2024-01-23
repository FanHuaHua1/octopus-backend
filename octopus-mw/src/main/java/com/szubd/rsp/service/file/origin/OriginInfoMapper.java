package com.szubd.rsp.service.file.origin;

import com.szubd.rsp.file.OriginInfo;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

import java.util.List;

@Mapper
@Repository
public interface OriginInfoMapper {
    List<OriginInfo> queryAll();
    List<OriginInfo> querySingle(int nodeId);

    List<OriginInfo> queryByName(String name);
    List<OriginInfo> queryAllFather();
    List<OriginInfo> queryAllFatherByNodeId(int nodeId);
    List<OriginInfo> queryBySuperName(String superName);
    List<OriginInfo> listFileDistribution(String superName);
    List<OriginInfo> queryBySuperNameAndId(String superName, String nodeId);
    int updateInfos(List<OriginInfo> originInfos);
    int deleteInfos(List<OriginInfo> originInfos);

}
