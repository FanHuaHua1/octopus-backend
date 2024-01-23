package com.szubd.rsp.dao;

import com.szubd.rsp.mapper.GlobalRSPInfoMapper;
import com.szubd.rsp.file.GlobalRSPInfo;
import com.szubd.rsp.service.init.NodeInfoQueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class GlobalRspDao {
    protected static final Logger logger = LoggerFactory.getLogger(GlobalRspDao.class);
    @Autowired
    GlobalRSPInfoMapper globalRSPInfoMapper;
    public int createNewFile(String superName, String rspName) {
        logger.info("创建事件（/rsp/father/son/grandson）");
        int hdfsFile = globalRSPInfoMapper.createHDFSFile(
                new GlobalRSPInfo(0, superName, rspName, 0, 0, 0, NodeInfoQueryService.nodeID, true, false, false));
        return hdfsFile;
    }
    public int insertFile(GlobalRSPInfo globalRSPInfo) {
        logger.info("插入改名事件");
        int hdfsFile = globalRSPInfoMapper.insertFile(globalRSPInfo);
        return hdfsFile;
    }

    public int insertFiles(List<GlobalRSPInfo> globalRSPInfo) {
        logger.info("插入改名事件s");
        int hdfsFile = globalRSPInfoMapper.insertFiles(globalRSPInfo);
        return hdfsFile;
    }

    public int deleteSonDirectoryTemp(String superName, String globalrspName) {
        logger.info("删除事件");
        int hdfsFile = globalRSPInfoMapper.deleteSonDirectoryTemp(new GlobalRSPInfo(0, superName, globalrspName, 0, 0,0, NodeInfoQueryService.nodeID,  true, true, true));
        return hdfsFile;
    }

////    public int deleteFatherDirectory(String superName) {
////        logger.info("删除事件");
////        int hdfsFile = rspInfoMapper.deleteFatherDirectory(superName);
////        return hdfsFile;
////    }
//
    public int deleteFatherDirectoryTemp(String superName) {
        logger.info("删除事件");
        int hdfsFile = globalRSPInfoMapper.deleteFatherDirectoryTemp(superName);
        return hdfsFile;
    }
//
//
//    public int deleteGrandSonDirectoryTemp(String superName, String name, String rspName) {
//        logger.info("删除事件");
//        int hdfsFile = localRSPInfoMapper.deleteGrandSonDirectoryTemp(new LocalRSPInfo(0, superName, name,  rspName, 0, 0,0, NodeInfoQueryService.nodeID,  true, true, true));
//        return hdfsFile;
//    }


    public int updateByName(String superName, String globalrspName, long len, int blocks) {
        logger.info("更新事件");
        int hdfsFile = globalRSPInfoMapper.updateHDFSFile(new GlobalRSPInfo(0, superName,globalrspName, blocks, len, blocks == 0? 0 : len / blocks, NodeInfoQueryService.nodeID, true, false, true));
        return hdfsFile;
    }
//
////    public List<RSPInfo> queryForModifiedFile() {
////        logger.info("查询更新过的文件");
////        return rspInfoMapper.queryForModified();
////    }
////
////    public int updateModifiedStatus(List<RSPInfo> rspInfos) {
////        logger.info("更新事件");
////        int hdfsFile = rspInfoMapper.updateModifiedStatus(rspInfos);
////        return hdfsFile;
////    }
//
//    public void updateFileByName(String oriSuperName, String oriName, String dstSuperName, String dstName) {
//        logger.info("根据父级名和子级名修改内容");
//        rspInfoMapper.updateFileByName(oriSuperName, oriName, dstSuperName, dstName);
//    }
//
//    public void updateFileBySuperName(String oriSuperName, String dstSuperName) {
//        logger.info("根据父级名和子级名修改内容");
//        rspInfoMapper.updateFileBySuperName(oriSuperName, dstSuperName);
//    }
//
    public List<GlobalRSPInfo> queryBySuperName(String superName){
        logger.info("根据父级名查询内容");
        return globalRSPInfoMapper.queryBySuperName(superName);
    }
//
    public GlobalRSPInfo queryBySuperNameAndGlobalrspName(String superName, String name){
        logger.info("根据父级名和子级名查询内容");
        return globalRSPInfoMapper.queryBySuperNameAndGlobalrspName(superName, name);
    }
//
//    public LocalRSPInfo queryBySuperNameAndNameAndLocalrspName(String superName, String name, String localrspName){
//        logger.info("根据父级名和子级名查询内容");
//        return localRSPInfoMapper.queryBySuperNameAndNameAndLocalrspName(superName, name, localrspName);
//    }

}
