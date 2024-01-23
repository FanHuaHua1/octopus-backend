package com.szubd.rsp.dao;

import com.szubd.rsp.mapper.LocalRSPInfoMapper;
import com.szubd.rsp.file.LocalRSPInfo;
import com.szubd.rsp.service.init.NodeInfoQueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class LocalRspDao {
    protected static final Logger logger = LoggerFactory.getLogger(LocalRspDao.class);
    @Autowired
    LocalRSPInfoMapper localRSPInfoMapper;

    public int createNewFile(String superName, String name) {
        logger.info("创建事件（/rsp/father/son/grandson）");
        int hdfsFile = localRSPInfoMapper.createHDFSFile(
                new LocalRSPInfo(0, superName, name, 0, 0, 0, NodeInfoQueryService.nodeID, true, false, false));
        return hdfsFile;
    }
    public int insertFile(LocalRSPInfo localRSPInfo) {
        logger.info("插入改名事件");
        int hdfsFile = localRSPInfoMapper.insertFile(localRSPInfo);
        return hdfsFile;
    }

    public int insertFiles(List<LocalRSPInfo> localRSPInfos) {
        logger.info("插入改名事件s");
        int hdfsFile = localRSPInfoMapper.insertFiles(localRSPInfos);
        return hdfsFile;
    }

    public int deleteSonDirectoryTemp(String superName, String name) {
        logger.info("删除事件");
        int hdfsFile = localRSPInfoMapper.deleteSonDirectoryTemp(new LocalRSPInfo(0, superName, name, 0, 0,0, NodeInfoQueryService.nodeID,  true, true, true));
        return hdfsFile;
    }

    public int deleteFatherDirectoryTemp(String superName) {
        logger.info("删除事件");
        int hdfsFile = localRSPInfoMapper.deleteFatherDirectoryTemp(superName);
        return hdfsFile;
    }


    public int updateByName(String superName, String name, long len, int blocks) {
        logger.info("更新事件");
        int hdfsFile = localRSPInfoMapper.updateHDFSFile(new LocalRSPInfo(0, superName, name, blocks, len, len / blocks, NodeInfoQueryService.nodeID, true, false, true));
        return hdfsFile;
    }

    public List<LocalRSPInfo> queryBySuperName(String superName){
        logger.info("根据父级名查询内容");
        return localRSPInfoMapper.queryBySuperName(superName);
    }

    public LocalRSPInfo queryBySuperNameAndName(String superName, String name){
        logger.info("根据父级名和子级名查询内容");
        return localRSPInfoMapper.queryBySuperNameAndName(superName, name);
    }

}
