package com.szubd.rsp.dao;

import com.szubd.rsp.mapper.OriginInfoMapper;
import com.szubd.rsp.file.OriginInfo;
import com.szubd.rsp.service.init.NodeInfoQueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.List;

@Component
public class OriginDao {
    protected static final Logger logger = LoggerFactory.getLogger(OriginDao.class);
    @Autowired
    OriginInfoMapper originInfoMapper;

    public int createNewFile(String superName, String name) {
        logger.info("创建事件（/rsp/father/son）");
        int hdfsFile = originInfoMapper.createHDFSFile(
                new OriginInfo(0, superName, name, 0, 0, 0, NodeInfoQueryService.nodeID, 0, 0, true, false, false));
        return hdfsFile;
    }
    public int insertFile(OriginInfo originInfo) {
        logger.info("插入改名事件");
        int hdfsFile = originInfoMapper.insertFile(originInfo);
        return hdfsFile;
    }

    public int insertFiles(List<OriginInfo> originInfos) {
        logger.info("插入改名事件s");
        int hdfsFile = originInfoMapper.insertFiles(originInfos);
        return hdfsFile;
    }

    public int deleteSonDirectoryTemp(String superName, String name) {
        logger.info("删除事件");
        int hdfsFile = originInfoMapper.deleteSonDirectoryTemp(new OriginInfo(0, superName, name,  0, 0, 0, NodeInfoQueryService.nodeID, 0, 0, true, true, true));
        return hdfsFile;
    }

//    public int deleteFatherDirectory(String superName) {
//        logger.info("删除事件");
//        int hdfsFile = rspInfoMapper.deleteFatherDirectory(superName);
//        return hdfsFile;
//    }

    public int deleteFatherDirectoryTemp(String superName) {
        logger.info("删除事件");
        int hdfsFile = originInfoMapper.deleteFatherDirectoryTemp(superName);
        return hdfsFile;
    }


    public int updateByName(String superName, String name, long len, int blocks) {
        logger.info("更新事件");
        int hdfsFile = originInfoMapper.updateHDFSFile(new OriginInfo(0, superName, name, blocks, len, len / blocks, NodeInfoQueryService.nodeID , 0, 0,  true, false, true));
        return hdfsFile;
    }

//    public List<RSPInfo> queryForModifiedFile() {
//        logger.info("查询更新过的文件");
//        return rspInfoMapper.queryForModified();
//    }
//
//    public int updateModifiedStatus(List<RSPInfo> rspInfos) {
//        logger.info("更新事件");
//        int hdfsFile = rspInfoMapper.updateModifiedStatus(rspInfos);
//        return hdfsFile;
//    }

    public void updateFileByName(String oriSuperName, String oriName, String dstSuperName, String dstName) {
        logger.info("根据父级名和子级名修改内容");
        originInfoMapper.updateFileByName(oriSuperName, oriName, dstSuperName, dstName);
    }

    public void updateFileBySuperName(String oriSuperName, String dstSuperName) {
        logger.info("根据父级名和子级名修改内容");
        originInfoMapper.updateFileBySuperName(oriSuperName, dstSuperName);
    }

    public List<OriginInfo> queryBySuperName(String superName){
        logger.info("根据父级名查询内容");
        return originInfoMapper.queryBySuperName(superName);
    }

    public OriginInfo queryBySuperNameAndName(String superName, String name){
        logger.info("根据父级名和子级名查询内容");
        return originInfoMapper.queryBySuperNameAndName(superName, name);
    }

}
