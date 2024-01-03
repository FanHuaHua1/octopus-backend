package com.szubd.rsp.service.init;

import com.szubd.rsp.hdfs.HadoopUtils;
import com.szubd.rsp.node.NodeInfoService;
import com.szubd.rsp.node.NodeInfo;
import org.apache.dubbo.config.annotation.DubboReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.net.InetAddress;


@Component
@Order(1)
public class NodeInfoQueryService implements CommandLineRunner {
    @Value("${hdfs.prefix}")
    private String generalPrefix;
    @Value("${spark-info.clusterConfig.clusterName}")
    private String clusterName;
    public static int nodeID = 0;
    @DubboReference(check = false)
    private NodeInfoService service;

    protected static final Logger logger = LoggerFactory.getLogger(NodeInfoQueryService.class);

    // 集群端初始化调用，向中间件注册本集群的信息
    @Override
    public void run(String... args) throws Exception {
        String hostAddress = InetAddress.getLocalHost().getHostAddress();
        nodeID = service.queryForNodeId(new NodeInfo(0, hostAddress, generalPrefix, HadoopUtils.getNameNodeAdress(), clusterName));
        logger.info("获取到本集群ID:" + nodeID);
    }
}
