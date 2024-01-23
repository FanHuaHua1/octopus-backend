package com.szubd.rsp.service.hdfs;

import com.szubd.rsp.file.OriginInfo;
import com.szubd.rsp.node.NodeInfoService;
import com.szubd.rsp.node.NodeService;
import com.szubd.rsp.service.file.origin.OriginInfoMapper;
import com.szubd.rsp.hdfs.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;
import com.szubd.rsp.service.node.NodeInfoMapper;
import com.szubd.rsp.node.NodeInfo;
import com.szubd.rsp.websocket.WebSocketServer;
import org.apache.dubbo.config.annotation.DubboReference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

@Controller
@RequestMapping("/hdfs")
public class HDFSController {
    @DubboReference(check = false, async = true)
    private NodeService service;
    @Autowired
    private HDFSService hdfsService;
    @Autowired
    private NodeInfoService nodeInfoService;
    @Autowired
    private JdbcTemplate template;
    @Autowired
    private WebSocketServer webSocketServer;
    @Autowired
    OriginInfoMapper originInfoMapper;
    @Autowired
    NodeInfoMapper nodeInfoMapper;

    protected static final Logger logger = LoggerFactory.getLogger(HDFSController.class);

    @ResponseBody
    @GetMapping("/listfiles")
    public List<OriginInfo> queryAllOriginFiles(){
        List<OriginInfo> originInfos = originInfoMapper.queryAll();
        return originInfos;
    }

    @ResponseBody
    @GetMapping("/listfile")
    public List<OriginInfo> listSingle(int nodeId){
        List<OriginInfo> originInfos = originInfoMapper.querySingle(nodeId);
        return originInfos;
    }

//    @ResponseBody
//    @GetMapping("/listblockpath2")
//    public String file2(Integer nodeId, String superName, String name) throws IOException, URISyntaxException, InterruptedException {
//        NodeInfo nodeInfo = nodeInfoMapper.queryForNodeInfoById(nodeId);
//        String ip = nodeInfo.getIp();
//        String prefix = nodeInfo.getPrefix();
//        String url = "hdfs://" + ip + ":8020";
//        String path = prefix + superName + "/" + name;
//        return HdfsUtils.getBlockStatus(url, path);
//        //return path;
//    }

    @ResponseBody
    @GetMapping("/listblockpath")
    public List<Map<String, Object>> file(Integer nodeId, String superName, String name) throws IOException, URISyntaxException, InterruptedException {
        NodeInfo nodeInfo = nodeInfoMapper.queryForNodeInfoById(nodeId);
        String ip = nodeInfo.getNameNodeIP();
        String prefix = nodeInfo.getPrefix();
        String url = "hdfs://" + ip + ":8020";
        String path = prefix + "origin/" + superName + "/" + name;
        return HadoopUtils.getBlockList(url, path, superName, name, nodeId);
    }

    @ResponseBody
    @GetMapping("/listhdfsblockpath")
    public List<Map<String, Object>> file3(Integer nodeId, String superName, String name, String blockName) throws IOException, URISyntaxException, InterruptedException {
        NodeInfo nodeInfo = nodeInfoMapper.queryForNodeInfoById(nodeId);
        String ip = nodeInfo.getNameNodeIP();
        String prefix = nodeInfo.getPrefix();
        String url = "hdfs://" + ip + ":8020";
        String path = prefix + "origin/" + superName + "/" + name + "/" + blockName;
        return HadoopUtils.getHDFSBlockAddress(url, path);
        //return path;
    }

    /**
     * 查询所有子级名字为name的数据
     * @param name
     * @return
     */
    @ResponseBody
    @GetMapping("/listfilebyname")
    public List<OriginInfo> listFileByName(String name) {
        List<OriginInfo> originInfos = originInfoMapper.queryByName(name);
        return originInfos;
    }

    /**
     * 查询所有父级名字为superName的数据
     * @param superName
     * @return
     */
    @ResponseBody
    @GetMapping("/listfilebysupername")
    public List<OriginInfo> listFileBySuperName(String superName) {
        List<OriginInfo> originInfos = originInfoMapper.queryBySuperName(superName);
        return originInfos;
    }
}
