package com.szubd.rsp.node;

import com.szubd.rsp.node.NodeInfo;

import java.io.IOException;


public interface NodeInfoService {
    int queryForNodeId(NodeInfo nodeInfo) throws IOException;
    NodeInfo queryForNodeInfoById(Integer id);
    NodeInfo queryForNodeInfoByIp(String ip);
}
