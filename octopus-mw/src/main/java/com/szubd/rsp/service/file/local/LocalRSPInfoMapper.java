package com.szubd.rsp.service.file.local;

import com.szubd.rsp.file.LocalRSPInfo;
import com.szubd.rsp.file.OriginInfo;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

import java.util.List;

@Mapper
@Repository
public interface LocalRSPInfoMapper {
    List<LocalRSPInfo> queryAll();
    List<LocalRSPInfo> querySingle(int nodeId);
    List<LocalRSPInfo> querySinglenew(int nodeId);
    int updateInfos(List<LocalRSPInfo> rspInfos);
    int deleteInfo(LocalRSPInfo rspInfo);
    int deleteInfos(List<LocalRSPInfo> rspInfos);

    List<LocalRSPInfo> listFileDistribution(String superName);
    List<LocalRSPInfo> queryAllFather();
    List<LocalRSPInfo> queryBySuperName(String superName);
    List<LocalRSPInfo> queryBySuperNameAndName(String superName, String name);
    List<LocalRSPInfo> queryAllFatherByNodeId(int nodeId);
    List<LocalRSPInfo> queryAllNameByNodeIdAndFather(int nodeId, String superName);
    List<LocalRSPInfo> queryAllNameByNodeIdAndFatherAndName(int nodeId, String superName, String name);
}
