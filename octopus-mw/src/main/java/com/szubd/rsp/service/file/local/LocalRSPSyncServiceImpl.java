package com.szubd.rsp.service.file.local;

import com.szubd.rsp.file.LocalRSPSyncService;
import com.szubd.rsp.file.LocalRSPInfo;
import org.apache.dubbo.config.annotation.DubboService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@DubboService
@Component
public class LocalRSPSyncServiceImpl implements LocalRSPSyncService {
    protected static final Logger logger = LoggerFactory.getLogger(LocalRSPSyncServiceImpl.class);
    @Autowired
    LocalRSPInfoMapper localRSPInfoMapper;

    @Override
    public int updateInfos(List<LocalRSPInfo> rspInfos) {
        logger.info("远端更新事件");
        int hdfsFile = localRSPInfoMapper.updateInfos(rspInfos);
        return hdfsFile;
    }

    @Override
    public int updateAndDeleteInfos(List<LocalRSPInfo> updateRspInfos, List<LocalRSPInfo> deleteRspInfos) {
        logger.info("远端更新并删除事件");
        localRSPInfoMapper.updateInfos(updateRspInfos);
        localRSPInfoMapper.deleteInfos(deleteRspInfos);
        return 1;
    }

    @Override
    public int deleteInfos(List<LocalRSPInfo> rspInfos) {
        logger.info("远端删除事件");
        localRSPInfoMapper.deleteInfos(rspInfos);
        return 1;
    }

}
