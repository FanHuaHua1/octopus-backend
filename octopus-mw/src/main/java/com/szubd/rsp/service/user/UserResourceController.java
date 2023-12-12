package com.szubd.rsp.service.user;

import com.alibaba.fastjson.JSONObject;
import com.sun.istack.internal.NotNull;
import com.szubd.rsp.http.Result;
import com.szubd.rsp.http.ResultCode;
import com.szubd.rsp.http.ResultResponse;
import com.szubd.rsp.service.job.JobLogoService;
import com.szubd.rsp.user.UserDubboService;
import com.szubd.rsp.user.UserResource;
import com.szubd.rsp.user.UserResourceDubboService;
import com.szubd.rsp.utils.JwtUtils;
import org.apache.dubbo.config.annotation.DubboReference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;

import static com.szubd.rsp.http.ResultCode.PARAMS_IS_INVALID;

/**
 * 用户资源管理类，包括：
 * 用户总资源查询
 * 用户总资源更新
 * -------
 * 待定：
 * 用户可用资源获取
 * @author leonardo
 */
@Controller
@RequestMapping("/resource")
public class UserResourceController {
    @DubboReference

    private UserResourceDubboService userResourceDubboService;

    @DubboReference
    private UserDubboService userDubboService;

    @Autowired
    private JwtUtils jwtUtils;
    @Autowired
    private JobLogoService jobLogoService;

    @ResponseBody
    @RequestMapping("/myResource")
    public Result queryUserResource(@NotNull String userId, HttpServletRequest request) {
        String token = request.getHeader("Authorization").replace("Bearer", "");
        if (!jwtUtils.parseJwt(token).getId().equals(userId)) {
            return ResultResponse.failure(ResultCode.UNAUTHORIZED, "Authorization does not match user");
        }
        // 获取该用户的资源配置
        UserResource userResource = userResourceDubboService.queryUserResourceByUserId(userId);
        return ResultResponse.success(userResource);
    }

    @ResponseBody
    @RequestMapping("/updateResource")
    public Result updateUserResource(@NotNull @RequestBody UserResource userResource) {
        System.out.println(userResource);
        String userId = userResource.getUserId();
        if (!checkUserExist(userId)) {
            return ResultResponse.failure(ResultCode.UNAUTHORIZED);
        }
        userResourceDubboService.updateUserResource(userResource);
        return ResultResponse.success();
    }

    //mai
    @ResponseBody
    @RequestMapping("/checkResource")
    public Result checkResource(@RequestBody JSONObject jsonObject) {
        String userId = jsonObject.getString("userId");
        int memory = Math.max(jsonObject.getIntValue("sparkExecutorMemory"),2);
        int cores = Math.max(jsonObject.getIntValue("sparkExecutorCores"),2);
        int executorNum = Math.max(jsonObject.getIntValue("sparkDynamicAllocationMaxExecutors"),2);
        int nodeId = jsonObject.getIntValue("nodeId");
        //获取用户资源
        UserResource userResource = userResourceDubboService.queryUserResourceByUserId(userId);
        //校验用户资源
        if((long) memory * 1024 * executorNum >= userResource.getMemory() || cores * executorNum >= userResource.getCpu()) {
            return ResultResponse.failure(PARAMS_IS_INVALID, "用户资源不足，无法执行任务");
        }
        //资源足够，检查系统资源
        boolean valid = jobLogoService.isValid(cores, executorNum, memory, nodeId);
        if(valid) {
            return ResultResponse.success("OK");
        } else {
            return ResultResponse.failure(PARAMS_IS_INVALID, "系统资源不足，请耐心等待");
        }


    }


    /**
     * 之后可以改为AOP获取cookie来检查用户登录信息
     * @param userId
     * @return
     */
    private Boolean checkUserExist(String userId) {
        return userDubboService.queryUserInfo(userId) != null;
    }

}
