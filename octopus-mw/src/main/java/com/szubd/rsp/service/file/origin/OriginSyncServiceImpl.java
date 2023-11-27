package com.szubd.rsp.service.file.origin;

import com.szubd.rsp.file.OriginSyncService;
import com.szubd.rsp.file.OriginInfo;
import org.apache.dubbo.config.annotation.DubboService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;


@DubboService
@Component
public class OriginSyncServiceImpl implements OriginSyncService {
    protected static final Logger logger = LoggerFactory.getLogger(OriginSyncServiceImpl.class);
    @Autowired
    OriginInfoMapper originInfoMapper;
    @Override
    public String sayHello(String name) {
        return "Hello " + name;
    }

//    @Override
//    public int createNewFile(String superName, String name, ) {
//        logger.info("创建事件");
//        int hdfsFile = rspInfoMapper.createHDFSFile(new RSPInfo(0, superName, name, 10, "13213", 43, 0, true, true, true, true));
//        return hdfsFile;
//    }
//
//
//    @Override
//    public int deleteFile(String superName, String name) {
//        logger.info("删除事件");
//        int hdfsFile = rspInfoMapper.deleteHDFSFile(new RSPInfo(0, superName, name, 10, "13213", 43, 0, true, true, true, true));
//        return hdfsFile;
//    }
//
//    @Override
//    public int updateByName(String superName, String name, long len, int blocks) {
//        logger.info("更新事件");
//        int hdfsFile = rspInfoMapper.updateHDFSFile(new RSPInfo(0, superName, name, blocks, "13213", 43, len , true, true, true, true));
//        return hdfsFile;
//    }

    @Override
    public int updateInfos(List<OriginInfo> originInfos) {
        logger.info("远端更新事件");
        int hdfsFile = originInfoMapper.updateInfos(originInfos);
        return hdfsFile;
    }

    @Override
    public int updateAndDeleteInfos(List<OriginInfo> updateOriginInfos, List<OriginInfo> deleteOriginInfos) {
        logger.info("远端更新并删除事件");
        originInfoMapper.updateInfos(updateOriginInfos);
        originInfoMapper.deleteInfos(deleteOriginInfos);
        return 1;
    }

    @Override
    public int deleteInfos(List<OriginInfo> originInfos) {
        logger.info("远端删除事件");
        originInfoMapper.deleteInfos(originInfos);
        return 1;
    }

}
