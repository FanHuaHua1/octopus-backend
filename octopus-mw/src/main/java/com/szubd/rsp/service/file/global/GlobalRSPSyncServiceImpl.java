package com.szubd.rsp.service.file.global;

import com.szubd.rsp.file.GlobalRSPSyncService;
import com.szubd.rsp.file.GlobalRSPInfo;
import org.apache.dubbo.config.annotation.DubboService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@DubboService
@Component
public class GlobalRSPSyncServiceImpl implements GlobalRSPSyncService {
    protected static final Logger logger = LoggerFactory.getLogger(GlobalRSPSyncServiceImpl.class);
    @Autowired
    GlobalRSPInfoMapper globalRSPInfoMapper;

    @Override
    public int updateInfos(List<GlobalRSPInfo> rspInfos) {
        logger.info("远端更新事件");
        logger.info("rspinfo:{}", rspInfos);
        int hdfsFile = globalRSPInfoMapper.updateInfos(rspInfos);
        return hdfsFile;
    }

    @Override
    public int updateAndDeleteInfos(List<GlobalRSPInfo> updateRspInfos, List<GlobalRSPInfo> deleteRspInfos) {
        logger.info("远端更新并删除事件");
        globalRSPInfoMapper.updateInfos(updateRspInfos);
        globalRSPInfoMapper.deleteInfos(deleteRspInfos);
        return 1;
    }

    @Override
    public int deleteInfos(List<GlobalRSPInfo> rspInfos) {
        logger.info("远端删除事件");
        globalRSPInfoMapper.deleteInfos(rspInfos);
        return 1;
    }

}
