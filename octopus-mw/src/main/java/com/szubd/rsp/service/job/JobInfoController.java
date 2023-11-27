package com.szubd.rsp.service.job;

import com.szubd.rsp.http.Result;
import com.szubd.rsp.http.ResultResponse;
import com.szubd.rsp.node.NodeService;
import com.szubd.rsp.service.node.NodeInfoMapper;
import com.szubd.rsp.job.JobInfo;
import com.szubd.rsp.websocket.WebSocketServer;
import org.apache.dubbo.config.annotation.DubboReference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.PostConstruct;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Controller
@RequestMapping("/job")
public class JobInfoController {

    @Autowired
    private JobService jobService;
    @Autowired
    private JdbcTemplate template;
    @Autowired
    private WebSocketServer webSocketServer;

//    @ResponseBody
//    @GetMapping("/listfiles")
//    public List<LocalRSPInfo> queryAllOriginFiles(){
//        List<LocalRSPInfo> rspInfos = localRSPInfoMapper.queryAll();
//        return rspInfos;
//    }

    @ResponseBody
    @GetMapping("/listall")
    public Result listAllJobs(){
        return ResultResponse.success(jobService.getJobInfoHashMap());
    }

    @ResponseBody
    @GetMapping("/list")
    public Result listJobs(){
        HashMap<Integer, JobInfo> jobInfoHashMap = jobService.getJobInfoHashMap();
        Stream<Map.Entry<Integer, JobInfo>> entryStream = jobInfoHashMap.entrySet().stream().filter(entry -> entry.getValue().getParentJobId() == -1);
        Stream<JobInfo> sorted = entryStream.map(entry -> entry.getValue()).sorted(Comparator.comparing(j -> j.getJobStartTime(), Comparator.reverseOrder()));
        //Stream<Map.Entry<Integer, JobInfo>> sorted = entryStream.sorted(Comparator.comparing(j -> j.getValue().getJobStartTime()));
        //Map<Integer, JobInfo> collect = sorted.collect(Collectors.toMap(item -> item.getKey(), item -> item.getValue()));
        List<JobInfo> collect = sorted.collect(Collectors.toList());
        return ResultResponse.success(collect);
    }

//    @ResponseBody
//    @GetMapping("/listsubjobs")
//    public Result listSubJobs(int parentId){
//        HashMap<Integer, JobInfo> jobInfoHashMap = jobService.getJobInfoHashMap();
//        Stream<JobInfo> jobInfoStream = jobInfoHashMap.entrySet().stream().filter(entry -> entry.getValue().getParentJobId() == parentId).map(entry -> entry.getValue());
//        Map<String, List<JobInfo>> jobGroup = jobInfoStream.collect(Collectors.groupingBy(JobInfo::getJobName));
//        Map<String, List<JobInfo>> collect2 = jobGroup.entrySet().stream().map(entry -> {
//            List<JobInfo> value = entry.getValue();
//            value.sort(Comparator.comparing(JobInfo::getJobStartTime));
//            entry.setValue(value);
//            return entry;
//        }).collect(Collectors.toMap(item -> item.getKey(), item -> item.getValue()));
//        return ResultResponse.success(collect2);
//    }
    @ResponseBody
    @GetMapping("/listsubjobs")
    public Result listSubJobs(int parentId){
        HashMap<Integer, JobInfo> jobInfoHashMap = jobService.getJobInfoHashMap();
        List<JobInfo> collect = jobInfoHashMap.entrySet().stream()
                .filter(entry -> entry.getValue().getParentJobId() == parentId)
                .map(entry -> entry.getValue())
                .sorted(Comparator.comparing(JobInfo::getJobStartTime))
                .collect(Collectors.toList());
        return ResultResponse.success(collect);
    }

    @PostConstruct
    private void warmConnectionPool() throws SQLException {
        template.getDataSource().getConnection();
        jobService.initJobId();
    }
}
