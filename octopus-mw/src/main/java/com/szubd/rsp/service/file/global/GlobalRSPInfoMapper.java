package com.szubd.rsp.service.file.global;

import com.szubd.rsp.file.GlobalRSPInfo;
import com.szubd.rsp.file.LocalRSPInfo;
import com.szubd.rsp.file.OriginInfo;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

import java.util.List;

@Mapper
@Repository
public interface GlobalRSPInfoMapper {
    List<GlobalRSPInfo> queryAll();
    List<GlobalRSPInfo> querySingle(int nodeId);
//    List<RSPInfo> queryByName(String name);
//    List<RSPInfo> queryBySuperName(String superName);
    int updateInfos(List<GlobalRSPInfo> rspInfos);
    int deleteInfos(List<GlobalRSPInfo> rspInfos);
    List<GlobalRSPInfo> queryAllFather();
    List<GlobalRSPInfo> queryBySuperName(String superName);
    List<GlobalRSPInfo> listFileDistribution(String superName);

    List<GlobalRSPInfo> queryAllNameByNodeIdAndFather(int nodeId, String superName);
//    List<RSPInfo> queryAllFather();
}
