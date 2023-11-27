package com.szubd.rsp.service.file.global;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.szubd.rsp.file.LocalRSPInfo;
import com.szubd.rsp.file.OriginInfo;
import com.szubd.rsp.node.NodeService;
import com.szubd.rsp.http.Result;
import com.szubd.rsp.http.ResultResponse;
import com.szubd.rsp.service.node.NodeInfoMapper;
import com.szubd.rsp.file.GlobalRSPInfo;
import com.szubd.rsp.node.NodeInfo;
import com.szubd.rsp.service.hdfs.HDFSService;
import com.szubd.rsp.websocket.WebSocketServer;
import com.szubd.rsp.hdfs.HadoopUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.dubbo.config.annotation.DubboReference;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/globalrsp")
public class GlobalRSPInfoController {

    @DubboReference(check = false, async = true)
    private NodeService service;
    @Autowired
    private HDFSService hdfsService;
    @Autowired
    private JdbcTemplate template;
    @Autowired
    private WebSocketServer webSocketServer;
    @Autowired
    GlobalRSPInfoMapper globalRSPInfoMapper;
    @Autowired
    NodeInfoMapper nodeInfoMapper;

    protected final static Logger logger = LoggerFactory.getLogger(GlobalRSPInfoController.class);

//    @ResponseBody
//    @GetMapping("/listfiles")
//    public List<GlobalRSPInfo> queryAllOriginFiles(){
//        List<GlobalRSPInfo> rspInfos = globalRSPInfoMapper.queryAll();
//        return rspInfos;
//    }

    @ResponseBody
    @GetMapping("/listfiles")
    public Result queryAllOriginFiles(){
        List<GlobalRSPInfo> rspInfos = globalRSPInfoMapper.queryAll();
        return ResultResponse.success(rspInfos);
    }

    @ResponseBody
    @GetMapping("/listfile")
    public List<GlobalRSPInfo> listSingle(int nodeId){
        List<GlobalRSPInfo> rspInfos = globalRSPInfoMapper.querySingle(nodeId);
        return rspInfos;
    }
//    @ResponseBody
//    @GetMapping("/listcategory")
//    public List<GlobalRSPInfo> listCategory() {
//        List<GlobalRSPInfo> globalRSPInfos = globalRSPInfoMapper.queryAllFather();
//        return globalRSPInfos;
//    }

    @ResponseBody
    @GetMapping("/listcategory")
    public Result listCategory() {
        List<GlobalRSPInfo> globalRSPInfos = globalRSPInfoMapper.queryAllFather();
        return ResultResponse.success(globalRSPInfos);
    }

    /**
     * 查询所有父级名字为superName的数据
     * @param superName
     * @return
     */
//    @ResponseBody
//    @GetMapping("/listfilebycategory")
//    public List<GlobalRSPInfo> listFileByCategory(String superName) {
//       List<GlobalRSPInfo> globalRSPInfos = globalRSPInfoMapper.queryBySuperName(superName);
//        return globalRSPInfos;
//    }


    @ResponseBody
    @GetMapping("/listfilebycategoryandnodeid")
    public Result listFileByCategoryAndNodeId(String superName, String nodeId) {
        List<GlobalRSPInfo> globalRSPInfos = globalRSPInfoMapper.queryAllNameByNodeIdAndFather(Integer.parseInt(nodeId), superName);
        return ResultResponse.success(globalRSPInfos);
    }

    @ResponseBody
    @GetMapping("/listfilebycategory")
    public Result listFileByCategory(String superName) {
       List<GlobalRSPInfo> globalRSPInfos = globalRSPInfoMapper.queryBySuperName(superName);
       return ResultResponse.success(globalRSPInfos);
    }

    @ResponseBody
    @GetMapping("/listfiledistribution")
    public Result listFileDistribution(String superName) {
        List<GlobalRSPInfo> globalRSPInfos = globalRSPInfoMapper.listFileDistribution(superName);
        return ResultResponse.success(globalRSPInfos);
    }

    @ResponseBody
    @GetMapping("/queryhdfsfile")
    public Result queryhdfsfile(String superName, String globalrspName, int nodeId) throws IOException, URISyntaxException, InterruptedException {
        Map<String, Object> map = new HashMap<>();
        NodeInfo nodeInfo = nodeInfoMapper.queryForNodeInfoById(nodeId);
        String ip = nodeInfo.getNameNodeIP();
        String prefix = nodeInfo.getPrefix();
        String url = "hdfs://" + ip + ":8020";
        String path = prefix + "globalrsp/" + superName + "/" + globalrspName;
        FileStatus fileStatus = HadoopUtils.getFileStatus(url, path);
        map.put("superName", superName);
        map.put("globalrspName", globalrspName);
        map.put("nodeId", nodeId);
        map.put("path", url + path);
        map.put("length", fileStatus.getLen());
//        map.put("replication", fileStatus.getReplication());
        map.put("blockSize", fileStatus.getBlockSize());
        map.put("modificationTime", fileStatus.getModificationTime());
        map.put("accessTime", fileStatus.getAccessTime());
        map.put("owner", fileStatus.getOwner());
        map.put("group", fileStatus.getGroup());
        map.put("permission", fileStatus.getPermission());
        return ResultResponse.success(map);
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class GlobalRSPInfos {
        public List<GlobalRSPInfo> data;
    }

    @ResponseBody
    @PostMapping("/checkfiles")
    public Result checkFiles(@RequestBody JSONObject jsonObject) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        GlobalRSPInfos globalRSPInfos = mapper.readValue(jsonObject.toJSONString(), GlobalRSPInfos.class);
        if (globalRSPInfos.data.size() != 0){
            logger.info("ok");
        }
        return ResultResponse.success("OK");
    }
}
