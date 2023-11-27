package com.szubd.rsp.service.resource;

import com.alibaba.nacos.api.naming.pojo.Instance;
import com.szubd.rsp.node.NodeInfo;

import com.szubd.rsp.service.node.NacosService;
import com.szubd.rsp.service.node.NodeInfoMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.szubd.rsp.service.resource.ClusterController.ips;

/*

@Component
@Slf4j
public class InitTask implements CommandLineRunner {
    @Autowired
    private NodeInfoMapper nodeInfoMapper;
    @Autowired
    private NacosService nacosService;
    @Override
    public void run(String... args){
        */
/* //把queryForNode_RmHostIp去了 mybatis mapper接口 resource上的xml
        List<NodeInfo> nodeInfos = nodeInfoMapper.queryForNode_RmHostIp();
        ips=new ArrayList<>();
        for(int i=0;i<nodeInfos.size();i++){
            ips.add(nodeInfos.get(i).getIp());
            log.info(ips.get(i));
        }*//*

        // 获取集群列表
        List<Instance> instances = null;
        try {
            instances = nacosService.listAliveCluster();
            // 获取ip集合：查询当前存活节点
            ips = instances.stream().map(Instance::getIp).collect(Collectors.toList());
            log.info("listAliveCluster:"+ips);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
*/
