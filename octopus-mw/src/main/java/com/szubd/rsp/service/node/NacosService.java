package com.szubd.rsp.service.node;

import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.dubbo.common.URL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
public class NacosService implements CommandLineRunner {
    @Value("${dubbo.registry.address}")
    private String NACOS_ADDRESS;
    @Autowired
    NodeInfoMapper nodeInfoMapper;
    protected static final Logger logger = LoggerFactory.getLogger(NacosService.class);

    @Override
    public void run(String... args) throws Exception {
//        NamingService naming = NamingFactory.createNamingService(NACOS_ADDRESS);
//        naming.subscribe("consumer", event -> {
//            if (event instanceof NamingEvent) {
//                logger.info("subscribe:{}", ((NamingEvent) event).getServiceName());
//                logger.info("subscribe:{}", ((NamingEvent) event).getInstances());
//            }
//        });
    }

    public List<Instance> listAliveCluster() throws Exception {
//        NamingService naming = NamingFactory.createNamingService(NACOS_ADDRESS);
//        List<Instance> allInstances = naming.getAllInstances("cluster");
//        return allInstances;
        return null;
    }
}
