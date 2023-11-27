package com.szubd.rsp.service.node;

import com.szubd.rsp.node.NodeInfo;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

import java.util.List;

@Mapper
@Repository
public interface NodeInfoMapper {
    Integer queryForNodeId(String ip);
    NodeInfo queryForNodeInfoById(Integer id);
    int updateNodeInfo(NodeInfo nodeInfo);
    int insertNewNode(NodeInfo nodeInfo);
    NodeInfo queryForNodeInfoByIp(String ip);
    /*List<NodeInfo> queryForNode_RmHostIp();*/
}
