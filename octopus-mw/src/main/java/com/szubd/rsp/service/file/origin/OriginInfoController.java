package com.szubd.rsp.service.file.origin;

import com.szubd.rsp.file.OriginInfo;
import com.szubd.rsp.node.NodeInfoService;
import com.szubd.rsp.node.NodeService;
import com.szubd.rsp.http.Result;
import com.szubd.rsp.http.ResultResponse;
import com.szubd.rsp.service.node.NodeInfoMapper;
import com.szubd.rsp.node.NodeInfo;
import com.szubd.rsp.service.hdfs.HDFSService;
import com.szubd.rsp.websocket.WebSocketServer;
import com.szubd.rsp.hdfs.HadoopUtils;
import org.apache.dubbo.config.annotation.DubboReference;
import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.*;

@Controller
@RequestMapping("/origin")
public class OriginInfoController {
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

    class ResponseObject implements Serializable {
    private int code;
    private List<OriginInfo> myList;

    public ResponseObject(int code, List<OriginInfo> myList) {
        this.code = code;
        this.myList = myList;
    }

    // getter 和 setter 方法省略
}
    @ResponseBody
    @GetMapping("/listfiles")
    public Result queryAllOriginFiles(){
        List<OriginInfo> originInfos = originInfoMapper.queryAll();
        return ResultResponse.success(originInfos);
    }
//    @ResponseBody
//    @GetMapping("/listfiles")
//    public List<RSPInfo> queryAllOriginFiles(){
//        List<RSPInfo> rspInfos = rspInfoMapper.queryAll();
//        return rspInfos;
//    }


    @ResponseBody
    @GetMapping("/listfile")
    public Result listSingle(int nodeId){
        List<OriginInfo> originInfos = originInfoMapper.querySingle(nodeId);
        return ResultResponse.success(originInfos);
    }

//    @ResponseBody
//    @GetMapping("/listcategory")
//    public List<RSPInfo> listCategory() {
//        List<RSPInfo> rspInfos = rspInfoMapper.queryAllFather();
//        return rspInfos;
//    }

    @ResponseBody
    @GetMapping("/listcategory")
    public Result listCategory() {
        List<OriginInfo> originInfos = originInfoMapper.queryAllFather();
        return ResultResponse.success(originInfos);
    }

    /**
     * 给rsp-logo用的
     * @return
     */
    @ResponseBody
    @GetMapping("/listcategorybyid")
    public Result listCategoryByNodeId(int nodeId) {
        List<OriginInfo> originInfos = originInfoMapper.queryAllFatherByNodeId(nodeId);
        return ResultResponse.success(originInfos);
    }


    /**
     * 查询所有父级名字为superName的数据
     * @param superName
     * @return
     */
//    @ResponseBody
//    @GetMapping("/listfilebycategory")
//    public List<RSPInfo> listFileByCategory(String superName) {
//        List<RSPInfo> rspInfos = rspInfoMapper.queryBySuperName(superName);
//        return rspInfos;
//    }

    @ResponseBody
    @GetMapping("/listfilebycategory")
    public Result listFileByCategory(String superName) {
        List<OriginInfo> originInfos = originInfoMapper.queryBySuperName(superName);
        return ResultResponse.success(originInfos);
    }


    @ResponseBody
    @GetMapping("/listfiledistribution")
    public Result listFileDistribution(String superName) {
        List<OriginInfo> originInfos = originInfoMapper.listFileDistribution(superName);
        return ResultResponse.success(originInfos);
    }

    @ResponseBody
    @GetMapping("/listfilebycategoryandid")
    public Result listFileByCategoryAndId(String superName, String nodeId) {
        List<OriginInfo> originInfos = originInfoMapper.queryBySuperNameAndId(superName, nodeId);
        return ResultResponse.success(originInfos);
    }

//    @ResponseBody
//    @GetMapping("/queryhdfsfile")
//    public Map<String, Object> queryhdfsfile(String superName, String name, int nodeId) throws IOException, URISyntaxException, InterruptedException {
//        Map<String, Object> map = new HashMap<>();
//        NodeInfo nodeInfo = nodeInfoMapper.queryForNodeInfoById(nodeId);
//        String ip = nodeInfo.getIp();
//        String prefix = nodeInfo.getPrefix();
//        String url = "hdfs://" + ip + ":8020";
//        String path = prefix + "origin/" + superName + "/" + name;
//        FileStatus fileStatus = HdfsUtils.getFileStatus(url, path);
//        map.put("superName", superName);
//        map.put("name", name);
//        map.put("nodeId", nodeId);
//        map.put("path", url + path);
//        map.put("length", fileStatus.getLen());
////        map.put("replication", fileStatus.getReplication());
//        map.put("blockSize", fileStatus.getBlockSize());
//        map.put("modificationTime", fileStatus.getModificationTime());
//        map.put("accessTime", fileStatus.getAccessTime());
//        map.put("owner", fileStatus.getOwner());
//        map.put("group", fileStatus.getGroup());
//        map.put("permission", fileStatus.getPermission());
//        return map;
//    }
@ResponseBody
    @GetMapping("/queryhdfsfile")
    public Result queryhdfsfile(String superName, String name, int nodeId) throws IOException, URISyntaxException, InterruptedException {
        Map<String, Object> map = new HashMap<>();
        NodeInfo nodeInfo = nodeInfoMapper.queryForNodeInfoById(nodeId);
        String ip = nodeInfo.getNameNodeIP();
        String prefix = nodeInfo.getPrefix();
        String url = "hdfs://" + ip + ":8020";
        String path = prefix + "origin/" + superName + "/" + name;
        FileStatus fileStatus = HadoopUtils.getFileStatus(url, path);
        map.put("superName", superName);
        map.put("name", name);
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

    @ResponseBody
    @GetMapping("/checkfiles")
    public Result checkFiles(){
        return ResultResponse.success("OK");
    }
}
