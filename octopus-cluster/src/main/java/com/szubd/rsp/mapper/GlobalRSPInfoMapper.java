package com.szubd.rsp.mapper;

import com.szubd.rsp.file.GlobalRSPInfo;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

import java.util.List;

@Mapper
@Repository
public interface GlobalRSPInfoMapper {
//    List<RSPInfo> queryAll();
    List<GlobalRSPInfo> queryBySuperName(String superName);
    GlobalRSPInfo queryBySuperNameAndGlobalrspName(String superName, String globalrspName);
    int createHDFSFile(GlobalRSPInfo info);
//    int deleteHDFSFile(RSPInfo info);
//    int deleteHDFSFileTemp(RSPInfo info);
    int updateHDFSFile(GlobalRSPInfo info);
    int insertFile(GlobalRSPInfo globalRSPInfo);
    int insertFiles(List<GlobalRSPInfo> globalRSPInfo);
    List<GlobalRSPInfo> queryModifiedGlobalRsp();
    List<GlobalRSPInfo> queryForDeleted();
    int updateModifiedStatus(List<GlobalRSPInfo> rspInfos);
    int updateDeletedStatus(List<GlobalRSPInfo> rspInfos);
////    int deleteFatherDirectory(String superName);
    int deleteInfo(GlobalRSPInfo info);
    int deleteFatherDirectoryTemp(String superName);
    int deleteSonDirectoryTemp(GlobalRSPInfo globalRSPInfo);
    int deleteGrandSonDirectoryTemp(GlobalRSPInfo globalRSPInfo);
//    void updateFileByName(String oriSuperName, String oriName, String dstSuperName, String dstName);
//    void updateFileBySuperName(String oriSuperName, String dstSuperName);

}
