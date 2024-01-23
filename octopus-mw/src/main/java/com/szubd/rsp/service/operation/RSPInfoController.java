package com.szubd.rsp.service.operation;

import com.szubd.rsp.algo.RspMixParams;
import com.szubd.rsp.algo.RspParams;
import com.szubd.rsp.file.OriginInfo;
import com.szubd.rsp.http.Result;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.szubd.rsp.node.NodeInfoService;
import com.szubd.rsp.node.NodeService;

import com.szubd.rsp.http.ResultResponse;
import com.szubd.rsp.service.file.origin.OriginInfoMapper;
import com.szubd.rsp.hdfs.HadoopUtils;
import com.szubd.rsp.service.hdfs.HDFSService;
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
@RequestMapping("/rsp")
public class RSPInfoController {
    @Autowired
    private RSPService rspService;
    @Autowired
    private WebSocketServer webSocketServer;
    @Autowired
    NodeInfoMapper nodeInfoMapper;

    protected static final Logger logger = LoggerFactory.getLogger(RSPInfoController.class);

    @ResponseBody
    @PostMapping("/torsp")
    public Result toRsp(@RequestBody RspParams rspParams) throws Exception {
        rspService.toRSPAction(rspParams.originName, rspParams.rspName, rspParams.blockNum, rspParams.originType, rspParams.nodeId);
        return ResultResponse.success();
    }

    @ResponseBody
    @PostMapping("/torspforlogo")
    public Result torspforlogo(@RequestBody RspParams rspParams) throws Exception {
        logger.info("ToRSP参数： {}", rspParams);
        rspService.toRSPAction(rspParams.originName, System.currentTimeMillis()+"-"+rspParams.blockNum, rspParams.blockNum, rspParams.originType, rspParams.nodeId);
        return ResultResponse.success();
    }

    @ResponseBody
    @PostMapping("/torspmix")
    public Result toRspMix(@RequestBody JSONObject jsonObject) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        RspMixParams rspMixParams = mapper.readValue(jsonObject.toJSONString(), RspMixParams.class);
        logger.info("RSP混洗参数： {}", rspMixParams);
        if (rspMixParams.data.size() != 0){
            //rspService.rspMixAction(rspMixParams);
            rspService.rspMixActionWithStrategy(rspMixParams);
        }
        return ResultResponse.success();
    }
}
