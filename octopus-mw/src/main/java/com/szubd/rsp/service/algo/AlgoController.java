package com.szubd.rsp.service.algo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.szubd.rsp.algo.AlgoInfo;
import com.szubd.rsp.file.GlobalRSPInfo;
import com.szubd.rsp.http.Result;
import com.szubd.rsp.http.ResultResponse;
import com.szubd.rsp.service.node.NacosService;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Controller
@Slf4j
@RequestMapping("/algo")
public class AlgoController {

    @Autowired
    private AlgoService algoService;

    private static final Logger logger = LoggerFactory.getLogger(AlgoController.class);

    @ResponseBody
    @GetMapping("/list")
    public Result list() throws Exception {
        return ResultResponse.success(algoService.list());
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class AlgoInfos {
        public List<GlobalRSPInfo> data;
        public String algoType;
        public String algo;
        public int runType;
        public double trainRatio;
        public double testRatio;
        public String labelCol;
        public String featureCol;
        public String itgStragegy;
        public int sparkExecutorCores;
        public int sparkExecutorMemory;
        public int sparkYarnMaxAppAttempts;
        public int sparkDynamicAllocationMaxExecutors;
        public String algoParams;
        public String executorsParams;
        public String expParams;

        @Override
        public String toString() {
            return "AlgoInfos{" +
                    "data=" + data +
                    ", algoType='" + algoType + '\'' +
                    ", algo='" + algo + '\'' +
                    ", runType=" + runType +
                    ", trainRatio=" + trainRatio +
                    ", testRatio=" + testRatio +
                    ", labelCol='" + labelCol + '\'' +
                    ", featureCol='" + featureCol + '\'' +
                    ", itgStragegy='" + itgStragegy + '\'' +
                    ", sparkExecutorCores=" + sparkExecutorCores +
                    ", sparkExecutorMemory=" + sparkExecutorMemory +
                    ", sparkYarnMaxAppAttempts=" + sparkYarnMaxAppAttempts +
                    ", sparkDynamicAllocationMaxExecutors=" + sparkDynamicAllocationMaxExecutors +
                    ", algoParams='" + algoParams + '\'' +
                    ", executorsParams='" + executorsParams + '\'' +
                    ", expParams='" + expParams + '\'' +
                    '}';
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class LogoAlgoInfos {
        public String superName;
        public String name;
        public int nodeId;
        public String algoType;
        public String algo;
        public double trainRatio;
        public double testRatio;
        public String labelCol;
        public String featureCol;
        public String itgStragegy;
        public int sparkExecutorCores;
        public int sparkExecutorMemory;
        public int sparkYarnMaxAppAttempts;
        public int sparkDynamicAllocationMaxExecutors;
        public String algoParams;
    }

    @ResponseBody
    @PostMapping("/submit")
    public Result crossDomainJob(@RequestBody JSONObject jsonObject) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        AlgoInfos algoInfos = mapper.readValue(jsonObject.toJSONString(), AlgoInfos.class);
        logger.info("receive job request: {}", algoInfos);
        String algoType = "";
        String algoSubSetting = "";
        switch(algoInfos.algo){
            case "Logistic Regression":{
                algoType = "clf";
                algoSubSetting = "logoShuffle";
                algoInfos.setAlgo("LR");
                break;
            }
            case "Random Forest":{
                algoType = "clf";
                algoSubSetting = "logoShuffle";
                algoInfos.setAlgo("RF");
                break;
            }
            case "Decision Tree":{
                algoType = "clf";
                algoSubSetting = "logoShuffle";
                algoInfos.setAlgo("DT");
                break;
            }
            case "SVM":{
                algoType = "clf";
                algoSubSetting = "logoShuffle";
                algoInfos.setAlgo("SVM");
                break;
            }
            case "Kmeans":{
                algoType = "clt";
                algoSubSetting = "logoShuffle";
                algoInfos.setAlgo("kmeans");
                break;
            }
            case "Bisecting Kmeans":{
                algoType = "clt";
                algoSubSetting = "logoShuffle";
                algoInfos.setAlgo("bisectingkmeans");
                break;
            }
            case "fpgrowth":{
                algoType = "fpg";
                algoSubSetting = "logoShuffle";
                algoInfos.setAlgo("Vote");
                break;
            }
            default:{
                break;
            }
        }
        Map<String, String> map = (Map<String, String>) JSON.parse(algoInfos.algoParams);
        logger.info("args map: {}", map);
        //algoService.submit(algoInfos, algoType, algoSubSetting, map);
        algoService.submitExp(algoInfos, algoType, algoSubSetting, map);
        return ResultResponse.success("OK");
    }


    @ResponseBody
    @PostMapping("/submitlogo")
    public Result checkFilesLogo(@RequestBody JSONObject jsonObject) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        LogoAlgoInfos logoAlgoInfos = mapper.readValue(jsonObject.toJSONString(), LogoAlgoInfos.class);
        //System.out.println(logoAlgoInfos);
        String algoType = "";
        String algoSubSetting = "";
        switch(logoAlgoInfos.algo){
            case "Logistic Regression":{
                algoType = "clf";
                algoSubSetting = "logoShuffle";
                logoAlgoInfos.setAlgo("LR");
                break;
            }
            case "Random Forest":{
                algoType = "clf";
                algoSubSetting = "logoShuffle";
                logoAlgoInfos.setAlgo("RF");
                break;
            }
            case "Decision Tree":{
                algoType = "clf";
                algoSubSetting = "logoShuffle";
                logoAlgoInfos.setAlgo("DT");
                break;
            }
            case "SVM":{
                algoType = "clf";
                algoSubSetting = "logoShuffle";
                logoAlgoInfos.setAlgo("SVM");
                break;
            }
            case "Kmeans":{
                algoType = "clt";
                algoSubSetting = "logoShuffle";
                logoAlgoInfos.setAlgo("kmeans");
                break;
            }
            case "Bisecting Kmeans":{
                algoType = "clt";
                algoSubSetting = "logoShuffle";
                logoAlgoInfos.setAlgo("bisectingkmeans");
                break;
            }
            case "fpgrowth":{
                algoType = "fpg";
                algoSubSetting = "logov";
                logoAlgoInfos.setAlgo("fpg");
                break;
            }
            default:{
                break;
            }
        }
        Map<String, String> map = (Map<String, String>) JSON.parse(logoAlgoInfos.algoParams);
        algoService.submitLogo(logoAlgoInfos, algoType, algoSubSetting, map);
        return ResultResponse.success("OK");
    }

    @ResponseBody
    @PostMapping("/add")
    public Result uploadNewAlgo(@RequestBody JSONObject jsonObject) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        AlgoInfo algoInfo = mapper.readValue(jsonObject.toJSONString(), AlgoInfo.class);
        algoService.uploadNewAlgo(algoInfo);
        return ResultResponse.success("OK");
    }
    // @ResponseBody必须得有：？   不是RestController
    @ResponseBody
    @PostMapping("/upload")
    public Result upload(MultipartFile file){
        log.info("[SPARK-INFO] 文件大小(MB):"+file.getSize()/(1024.0*1024.0));
        int num = algoService.upload(file);
        assert num==1;
        return ResultResponse.success();
    }
}
