package com.szubd.rsp.service.resource.service;

import com.szubd.rsp.resource.ClusterResourceService;
import com.szubd.rsp.resource.bean.ClusterInfo;
import com.szubd.rsp.resource.bean.ClusterMetrics;
import com.szubd.rsp.resource.bean.ClusterNode;
import com.szubd.rsp.service.resource.bean.Cluster;
import com.szubd.rsp.service.resource.dao.ApiDao;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.dubbo.config.annotation.DubboService;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;


@Service
@Data
@DubboService
@Slf4j
public class ClusterResourceServiceIml implements ClusterResourceService {
    @Autowired
    private Cluster cluster;

    @Autowired
    private ApiDao apiDao;

    @Scheduled(fixedRate = 10000)
    public void initOrUpdate() {
        // 配置文件转化的集合拉过来 一定要注意程序的执行顺序 避免空指针异常
//        log.info("[SPARK-INFO] 资源信息更新");
        apiDao.initOrUpdate();
//        log.info("[SPARK-INFO] 资源信息更新完成");
    }

    @Override
    public ClusterMetrics getClusterMetricsInfo() {
        return apiDao.getClusterMetricsInfo();
    }
    @Override
    public ClusterInfo getClusterInfo() {
        return apiDao.getClusterInfo();
    }
    @Override
    public List<ClusterNode> getClusterNodesInfo(){
        return apiDao.getClusterNodesInfo();
    }
}
