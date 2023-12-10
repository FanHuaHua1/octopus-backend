package com.szubd.rsp.controller;

import com.szubd.rsp.http.Result;
import com.szubd.rsp.http.ResultCode;
import com.szubd.rsp.http.ResultResponse;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * 任务相关接口，暂时预留
 * @author leonardo
 */
@Controller
@RequestMapping("/task")
public class TaskController {

    @RequestMapping("/myTasks")
    public Result queryUserTaskByUserId(String userId) {
        return ResultResponse.success();
    }

    @RequestMapping("/runningTasks")
    public Result queryRunningTasks(String userId) {
        return ResultResponse.success();
    }

}
