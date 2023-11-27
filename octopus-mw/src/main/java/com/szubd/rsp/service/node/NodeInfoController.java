package com.szubd.rsp.service.node;

import com.alibaba.nacos.api.naming.pojo.Instance;
import com.szubd.rsp.node.NodeInfoService;
import com.szubd.rsp.http.ResultCode;
import com.szubd.rsp.http.ResultResponse;
import com.szubd.rsp.service.hdfs.HDFSService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Controller
@RequestMapping("/system")
public class NodeInfoController {
    @Autowired
    private HDFSService hdfsService;
    @Autowired
    private NodeInfoService nodeInfoService;
    @Autowired
    private NacosService nacosService;
    private static final Logger logger = LoggerFactory.getLogger(NodeInfoController.class);

    @GetMapping("/checkIfNodeAliveAndDirectoryNotExist")
    @ResponseBody
    public boolean checkIfNodeAliveAndDirectoryNotExist(Integer nodeId, String superName, String name) throws Exception {
        boolean isNodeAlive = true;
        boolean isDirectoryNotExist = !hdfsService.checkIfNodeAliveAndDirectoryExist(nodeId, superName, name);
        return isNodeAlive && isDirectoryNotExist;
    }
    @ResponseBody
    @GetMapping("/list")
    public Object list() throws Exception {
        return ResultResponse.success(nacosService.listAliveCluster());
    }

    @ResponseBody
    @GetMapping("/checkclusters")
    public Object checkclusters(String params) throws Exception {
        logger.info(params);
        Set<String> ipSet = Arrays.stream(params.split(":")).map(id -> nodeInfoService.queryForNodeInfoById(Integer.parseInt(id)).getIp()).collect(Collectors.toSet());
        List<Instance> instances = nacosService.listAliveCluster();
        Set<String> aliveIpSet = instances.stream().map(instance -> instance.getIp()).collect(Collectors.toSet());
        ipSet.removeAll(aliveIpSet);
        if (ipSet.size() == 0) {
            return ResultResponse.success();
        } else {
            return ResultResponse.failure(ResultCode.CLUSTERS_NOT_ALIVE);
        }
    }
}
