package com.szubd.rsp.service.file.local;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.szubd.rsp.file.CDDubboService;
import com.szubd.rsp.file.OriginInfo;
import com.szubd.rsp.http.ResultCode;
import com.szubd.rsp.node.NodeInfoService;
import com.szubd.rsp.node.NodeService;
import com.szubd.rsp.http.Result;
import com.szubd.rsp.http.ResultResponse;
import com.szubd.rsp.service.node.NodeInfoMapper;
import com.szubd.rsp.file.LocalRSPInfo;
import com.szubd.rsp.node.NodeInfo;
import com.szubd.rsp.service.hdfs.HDFSService;
import com.szubd.rsp.tools.DubboUtils;
import com.szubd.rsp.websocket.WebSocketServer;
import com.szubd.rsp.hdfs.HadoopUtils;
import org.apache.dubbo.config.ReferenceConfig;
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
import java.util.*;

@Controller
@RequestMapping("/localrsp")
public class LocalRSPInfoController {

    @DubboReference(check = false, async = true)
    private NodeService service;
    @Autowired
    private HDFSService hdfsService;
    @Autowired
    private JdbcTemplate template;
    @Autowired
    private WebSocketServer webSocketServer;
    @Autowired
    LocalRSPInfoMapper localRSPInfoMapper;
    @Autowired
    private NodeInfoService nodeInfoService;
    @Autowired
    NodeInfoMapper nodeInfoMapper;

    protected final static Logger logger = LoggerFactory.getLogger(LocalRSPInfoController.class);

//    @ResponseBody
//    @GetMapping("/listfiles")
//    public List<LocalRSPInfo> queryAllOriginFiles(){
//        List<LocalRSPInfo> rspInfos = localRSPInfoMapper.queryAll();
//        return rspInfos;
//    }

    @ResponseBody
    @GetMapping("/listfiles")
    public Result queryAllOriginFiles(){
        List<LocalRSPInfo> rspInfos = localRSPInfoMapper.queryAll();
        return ResultResponse.success(rspInfos);
    }


    @ResponseBody
    @GetMapping("/listfile")
    public Result listSingle(int nodeId){
        List<LocalRSPInfo> rspInfos = localRSPInfoMapper.querySingle(nodeId);
        return ResultResponse.success(rspInfos);
    }

    @ResponseBody
    @GetMapping("/listfilenew")
    public List<LocalRSPInfo> listSinglenew(int nodeId){
        List<LocalRSPInfo> rspInfos = localRSPInfoMapper.querySinglenew(nodeId);
        return rspInfos;
    }
//    @ResponseBody
//    @GetMapping("/listcategory")
//    public List<LocalRSPInfo> listCategory() {
//        List<LocalRSPInfo> localRSPInfos = localRSPInfoMapper.queryAllFather();
//        return localRSPInfos;
//    }

    @ResponseBody
    @GetMapping("/listcategory")
    public Result listCategory() {
        List<LocalRSPInfo> localRSPInfos = localRSPInfoMapper.queryAllFather();
        return ResultResponse.success(localRSPInfos);
    }

    /**
     * 查询所有父级名字为superName的数据
     * @param superName
     * @return
     */
//    @ResponseBody
//    @GetMapping("/listfilebycategory")
//    public List<LocalRSPInfo> listFileByCategory(String superName) {
//       List<LocalRSPInfo> localRSPInfos = localRSPInfoMapper.queryBySuperName(superName);
//       return localRSPInfos;
//    }

    @ResponseBody
    @GetMapping("/listfilebycategory")
    public Result listFileByCategory(String superName) {
       List<LocalRSPInfo> localRSPInfos = localRSPInfoMapper.queryBySuperName(superName);
       return ResultResponse.success(localRSPInfos);
    }

//    @ResponseBody
//    @GetMapping("/listfilebycategoryandname")
//    public List<LocalRSPInfo> listFileByCategoryAndName(String superName, String name) {
//        System.out.println(superName + "--" + name);
//       List<LocalRSPInfo> localRSPInfos = localRSPInfoMapper.queryBySuperNameAndName(superName, name);
//       return localRSPInfos;
//    }
    @ResponseBody
    @GetMapping("/listfilebycategoryandname")
    public Result listFileByCategoryAndName(String superName, String name) {
        List<LocalRSPInfo> localRSPInfos = localRSPInfoMapper.queryBySuperNameAndName(superName, name);
        return ResultResponse.success(localRSPInfos);
    }
    @ResponseBody
    @GetMapping("/listfiledistribution")
    public Result listFileDistribution(String superName) {
        List<LocalRSPInfo> localRSPInfos = localRSPInfoMapper.listFileDistribution(superName);
        return ResultResponse.success(localRSPInfos);
    }

    @ResponseBody
    @GetMapping("/listfilebycategoryandnodeid")
    public Result listFileByCategoryAndNodeId(String superName, String nodeId) {
        List<LocalRSPInfo> localRSPInfos = localRSPInfoMapper.queryAllNameByNodeIdAndFather(Integer.parseInt(nodeId), superName);
        return ResultResponse.success(localRSPInfos);
    }
    /**
     * 给rsp-logo用的
     * @return
     */
    @ResponseBody
    @GetMapping("/listcategorybyid")
    public Result listCategoryByNodeId(int nodeId) {
        List<LocalRSPInfo> localRSPInfos = localRSPInfoMapper.queryAllFatherByNodeId(nodeId);
        return ResultResponse.success(localRSPInfos);
    }

    @ResponseBody
    @GetMapping("/listfilebycategoryandid")
    public Result listFileByCategoryAndId(String superName,int nodeId) {
       List<LocalRSPInfo> localRSPInfos = localRSPInfoMapper.queryAllNameByNodeIdAndFather(nodeId, superName);
       return ResultResponse.success(localRSPInfos);
    }

    @ResponseBody
    @GetMapping("/listfilebycategoryandnameandid")
    public Result listFileByCategoryAndNameAndId(String superName, String name,int nodeId) {
        List<LocalRSPInfo> localRSPInfos = localRSPInfoMapper.queryAllNameByNodeIdAndFatherAndName(nodeId, superName, name);
        return ResultResponse.success(localRSPInfos);
    }


    @ResponseBody
    @GetMapping("/queryhdfsfile")
    public Result queryhdfsfile(String superName, String name, int nodeId) throws IOException, URISyntaxException, InterruptedException {
        Map<String, Object> map = new HashMap<>();
        NodeInfo nodeInfo = nodeInfoMapper.queryForNodeInfoById(nodeId);
        String ip = nodeInfo.getNameNodeIP();
        String prefix = nodeInfo.getPrefix();
        String url = "hdfs://" + ip + ":8020";
        String path = prefix + "localrsp/" + superName + "/" + name;
        FileStatus fileStatus = HadoopUtils.getFileStatus(url, path);
        map.put("superName", superName);
        map.put("name", name);
        map.put("nodeId", nodeId);
        map.put("path", url + path);
        map.put("length", fileStatus.getLen());
        map.put("blockSize", fileStatus.getBlockSize());
        map.put("modificationTime", fileStatus.getModificationTime());
        map.put("accessTime", fileStatus.getAccessTime());
        map.put("owner", fileStatus.getOwner());
        map.put("group", fileStatus.getGroup());
        map.put("permission", fileStatus.getPermission());
        return ResultResponse.success(map);
    }

    @ResponseBody
    @PostMapping("/rm")
    public Result toRspMix(@RequestBody JSONObject jsonObject) throws Exception {
        Map<String, String> paramsMap = JSONObject.parseObject(jsonObject.toJSONString(), new TypeReference<Map<String, String>>(){});
        NodeInfo nodeInfo = nodeInfoService.queryForNodeInfoById(Integer.parseInt(paramsMap.get("nodeId")));
        String ip = nodeInfo.getNameNodeIP();
        CDDubboService cdDubboService = DubboUtils.getServiceRef(ip, "com.szubd.rsp.file.CDDubboService");
//        ReferenceConfig<CDDubboService> referenceConfig = new ReferenceConfig<>();
//        referenceConfig.setInterface("com.szubd.rsp.file.CDDubboService");
//        referenceConfig.setUrl("dubbo://"+ip+":20880/com.szubd.rsp.file.CDDubboService");
//        CDDubboService cdDubboService = referenceConfig.get();
        boolean isSuccess = cdDubboService.rmFile(1, paramsMap.get("superName") + "/" + paramsMap.get("name"));
        if(isSuccess) {
            localRSPInfoMapper.deleteInfo(
                    new LocalRSPInfo(
                            -1,
                            paramsMap.get("superName"),
                            paramsMap.get("name"),
                            -1,
                            -1,
                            -1,
                            Integer.parseInt(paramsMap.get("nodeId")),
                            false,
                            false,
                            false)
            );
            return ResultResponse.success();
        }
        else
            return ResultResponse.failure(ResultCode.NOT_FOUND, "删除失败");
    }
}
