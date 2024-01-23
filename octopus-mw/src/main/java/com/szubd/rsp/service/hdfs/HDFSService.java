package com.szubd.rsp.service.hdfs;

import com.szubd.rsp.algo.OperationDubboService;
import com.szubd.rsp.algo.RspMixParams;
import com.szubd.rsp.node.NodeInfoService;
import com.szubd.rsp.job.JobInfo;
import com.szubd.rsp.service.job.JobService;
import com.szubd.rsp.node.NodeInfo;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.annotation.DubboReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class HDFSService {

    @DubboReference(check = false, async = true, parameters = {"router", "address"})
    private OperationDubboService service;
    @Autowired
    private JobService jobService;
    @Autowired
    private NodeInfoService nodeInfoService;

    protected static final Logger logger = LoggerFactory.getLogger(HDFSService.class);

    public boolean checkIfNodeAliveAndDirectoryExist(Integer nodeId, String superName, String name) throws Exception {
        NodeInfo nodeInfo = nodeInfoService.queryForNodeInfoById(nodeId);
        String ip = nodeInfo.getNameNodeIP();
        String prefix = nodeInfo.getPrefix();
        String url = "hdfs://" + ip + ":8020";
        String path = prefix + superName + "/" + name + "/";
        return isPathExist(url, path);
    }


    public static boolean isPathExist(String url, String path) throws Exception {
        URI uri = new URI(url);
        Configuration conf =  new Configuration();
        String user = "zhaolingxiang";
        FileSystem fileSystem = FileSystem.get(uri, conf, user);
        return fileSystem.exists(new Path(path));
    }
}
