package com.szubd.rsp.mapper;

import com.szubd.rsp.file.OriginInfo;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

import java.util.List;

@Mapper
@Repository
public interface OriginInfoMapper {
    List<OriginInfo> queryAll();
    List<OriginInfo> queryBySuperName(String superName);
    OriginInfo queryBySuperNameAndName(String superName, String name);
    int createHDFSFile(OriginInfo info);
//    int deleteHDFSFile(RSPInfo info);
    int deleteInfo(OriginInfo info);
    int updateHDFSFile(OriginInfo info);
    int insertFile(OriginInfo originInfo);
    int insertFiles(List<OriginInfo> originInfos);
    List<OriginInfo> queryForModified();
    List<OriginInfo> queryForDeleted();
    int updateModifiedStatus(List<OriginInfo> originInfos);
    int updateDeletedStatus(List<OriginInfo> originInfos);
//    int deleteFatherDirectory(String superName);
    int deleteFatherDirectoryTemp(String superName);
    int deleteSonDirectoryTemp(OriginInfo info);
    void updateFileByName(String oriSuperName, String oriName, String dstSuperName, String dstName);
    void updateFileBySuperName(String oriSuperName, String dstSuperName);

}
