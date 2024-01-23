package com.szubd.rsp.service.node;

import com.szubd.rsp.node.NodeInfoService;
import com.szubd.rsp.node.NodeInfo;
import com.szubd.rsp.websocket.WebSocketServer;
import org.apache.dubbo.config.annotation.DubboService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

@DubboService
@Component
public class NodeInfoServiceImpl implements NodeInfoService {
    protected static final Logger logger = LoggerFactory.getLogger(NodeInfoServiceImpl.class);
    @Autowired
    NodeInfoMapper nodeInfoMapper;
    @Autowired
    private WebSocketServer webSocketServer;

    @Override
    public int queryForNodeId(NodeInfo nodeInfo) throws IOException {
        Integer id = nodeInfoMapper.queryForNodeId(nodeInfo.getIp());
        if(id == null){
            nodeInfoMapper.insertNewNode(nodeInfo);
            id = nodeInfo.getId();
        } else {
            NodeInfo curNodeInfo = nodeInfoMapper.queryForNodeInfoById(id);
            if(curNodeInfo.getNameNodeIP() != nodeInfo.getNameNodeIP()){
                nodeInfoMapper.updateNodeInfo(nodeInfo);
            }
        }
        logger.info("有服务节点加入：{}", nodeInfo.getIp());
        webSocketServer.sendMessage("服务节点：" + nodeInfo.getIp()); //没执行？
        return id;
    }

    @Override
    public NodeInfo queryForNodeInfoById(Integer id) {
        NodeInfo nodeInfo = nodeInfoMapper.queryForNodeInfoById(id);
        return nodeInfo;
    }

    @Override
    public NodeInfo queryForNodeInfoByIp(String ip) {
        NodeInfo nodeInfo = nodeInfoMapper.queryForNodeInfoByIp(ip);
        return nodeInfo;
    }
}
