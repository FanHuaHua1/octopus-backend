package com.szubd.rsp.service.node;

import com.szubd.rsp.file.GlobalRSPSyncService;
import com.szubd.rsp.file.LocalRSPSyncService;
import com.szubd.rsp.node.NodeService;
import com.szubd.rsp.file.OriginSyncService;
import com.szubd.rsp.mapper.GlobalRSPInfoMapper;
import com.szubd.rsp.mapper.LocalRSPInfoMapper;
import com.szubd.rsp.mapper.OriginInfoMapper;
import com.szubd.rsp.file.GlobalRSPInfo;
import com.szubd.rsp.file.LocalRSPInfo;
import com.szubd.rsp.file.OriginInfo;
import com.szubd.rsp.service.init.NodeInfoQueryService;
import org.apache.dubbo.config.annotation.DubboReference;
import org.apache.dubbo.config.annotation.DubboService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.List;


@Component
@DubboService
public class NodeServiceImpl implements NodeService {

    protected static final Logger logger = LoggerFactory.getLogger(NodeServiceImpl.class);
    @DubboReference(check = false)
    private OriginSyncService originSyncService;
    @DubboReference(check = false)
    private LocalRSPSyncService lservice;
    @DubboReference(check = false)
    private GlobalRSPSyncService gservice;
    @Autowired
    OriginInfoMapper originInfoMapper;
    @Autowired
    LocalRSPInfoMapper localRSPInfoMapper;
    @Autowired
    GlobalRSPInfoMapper globalRSPInfoMapper;

    @Scheduled(cron = "0/20 * * * * ?")
    public void testScheduling() {
        if (NodeInfoQueryService.nodeID == 0)
            return;
        //查询需要更新的
        List<OriginInfo> originInfosForUpdate = originInfoMapper.queryForModified();
        //查询需要删除的
        List<OriginInfo> originInfosForDelete = originInfoMapper.queryForDeleted();
        boolean requiredUpdate = (originInfosForUpdate == null) || (originInfosForUpdate.size() == 0);
        boolean requiredDelete = (originInfosForDelete == null) || (originInfosForDelete.size() == 0);

        if (requiredUpdate && requiredDelete) {
            //logger.info("无需更新");
            return;
        } else if ((!requiredUpdate) && (!requiredDelete)) {
            originSyncService.updateAndDeleteInfos(originInfosForUpdate, originInfosForDelete);
            originInfoMapper.updateModifiedStatus(originInfosForUpdate);
            originInfoMapper.updateDeletedStatus(originInfosForDelete);
        } else if (!requiredUpdate) {
            originSyncService.updateInfos(originInfosForUpdate);
            originInfoMapper.updateModifiedStatus(originInfosForUpdate);
        } else {
            originSyncService.deleteInfos(originInfosForDelete);
            originInfoMapper.updateDeletedStatus(originInfosForDelete);
        }
        logger.info("定时更新完成");
    }

    @Scheduled(cron = "0/20 * * * * ?")
    public void testScheduling2() {
        if (NodeInfoQueryService.nodeID == 0)
            return;
//        System.out.println("集群ID" + NodeInfoQueryService.nodeID);
        //查询需要更新的
        List<LocalRSPInfo> rspInfosForUpdate = localRSPInfoMapper.queryModifiedLocalRsp();
        //查询需要删除的
        List<LocalRSPInfo> rspInfosForDelete = localRSPInfoMapper.queryForDeleted();
        boolean requiredUpdate = (rspInfosForUpdate == null) || (rspInfosForUpdate.size() == 0);
        boolean requiredDelete = (rspInfosForDelete == null) || (rspInfosForDelete.size() == 0);

        if (requiredUpdate && requiredDelete) {
            //logger.info("localrsp无需更新");
            return;
        } else if ((!requiredUpdate) && (!requiredDelete)) {
            lservice.updateAndDeleteInfos(rspInfosForUpdate, rspInfosForDelete);
            localRSPInfoMapper.updateModifiedStatus(rspInfosForUpdate);
            localRSPInfoMapper.updateDeletedStatus(rspInfosForDelete);
        } else if (!requiredUpdate) {
            lservice.updateInfos(rspInfosForUpdate);
            localRSPInfoMapper.updateModifiedStatus(rspInfosForUpdate);
        } else {
            lservice.deleteInfos(rspInfosForDelete);
            localRSPInfoMapper.updateDeletedStatus(rspInfosForDelete);
        }
        logger.info("定时更新完成");
    }

    @Scheduled(cron = "0/20 * * * * ?")
    public void testScheduling3() {
        if (NodeInfoQueryService.nodeID == 0)
            return;
        List<GlobalRSPInfo> rspInfosForUpdate = globalRSPInfoMapper.queryModifiedGlobalRsp();
        List<GlobalRSPInfo> rspInfosForDelete = globalRSPInfoMapper.queryForDeleted();

        boolean requiredUpdate = (rspInfosForUpdate == null) || (rspInfosForUpdate.size() == 0);
        boolean requiredDelete = (rspInfosForDelete == null) || (rspInfosForDelete.size() == 0);

        if (requiredUpdate && requiredDelete) {
            //logger.info("localrsp无需更新");
            return;
        } else if ((!requiredUpdate) && (!requiredDelete)) {
            gservice.updateAndDeleteInfos(rspInfosForUpdate, rspInfosForDelete);
            globalRSPInfoMapper.updateModifiedStatus(rspInfosForUpdate);
            globalRSPInfoMapper.updateDeletedStatus(rspInfosForDelete);
        } else if (!requiredUpdate) {
            gservice.updateInfos(rspInfosForUpdate);
            globalRSPInfoMapper.updateModifiedStatus(rspInfosForUpdate);
        } else {
            gservice.deleteInfos(rspInfosForDelete);
            globalRSPInfoMapper.updateDeletedStatus(rspInfosForDelete);
        }
        logger.info("定时更新完成");
    }

    @Override
    public List<OriginInfo> queryModifiedFile() {
        return null;
    }

    @Override
    public List<LocalRSPInfo> queryModifiedLocalRspFile() {
        return null;
    }

    @Override
    public List<GlobalRSPInfo> queryModifiedGlobalRspFile() {
        return null;
    }


}
