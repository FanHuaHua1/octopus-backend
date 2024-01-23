package com.szubd.rsp.mapper;

import com.szubd.rsp.file.LocalRSPInfo;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

import java.util.List;

@Mapper
@Repository
public interface LocalRSPInfoMapper {
    List<LocalRSPInfo> queryBySuperName(String superName);
    LocalRSPInfo  queryBySuperNameAndName(String superName, String name);
    int createHDFSFile(LocalRSPInfo info);
    int deleteInfo(LocalRSPInfo info);
    int updateHDFSFile(LocalRSPInfo info);
    int insertFile(LocalRSPInfo localRSPInfo);
    int insertFiles(List<LocalRSPInfo> localRSPInfos);
    List<LocalRSPInfo> queryModifiedLocalRsp();
    List<LocalRSPInfo> queryForDeleted();
    int updateModifiedStatus(List<LocalRSPInfo> rspInfos);
    int updateDeletedStatus(List<LocalRSPInfo> rspInfos);
    int deleteFatherDirectoryTemp(String superName);
    int deleteSonDirectoryTemp(LocalRSPInfo localRSPInfo);

}
