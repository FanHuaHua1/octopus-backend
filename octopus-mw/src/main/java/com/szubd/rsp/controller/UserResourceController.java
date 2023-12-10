package com.szubd.rsp.controller;

import com.sun.istack.internal.NotNull;
import com.szubd.rsp.http.Result;
import com.szubd.rsp.http.ResultCode;
import com.szubd.rsp.http.ResultResponse;
import com.szubd.rsp.pojo.resourcePojo.UserResource;
import com.szubd.rsp.service.user.UserResourceService;
import com.szubd.rsp.service.user.UserService;
import com.szubd.rsp.utils.JwtUtils;
import org.apache.dubbo.config.annotation.DubboReference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;

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
    private UserResourceService userResourceService;

    @DubboReference
    private UserService userService;

    @Autowired
    private JwtUtils jwtUtils;

    @ResponseBody
    @RequestMapping("/myResource")
    public Result queryUserResource(@NotNull String userId, HttpServletRequest request) {
        String token = request.getHeader("Authorization").replace("Bearer", "");
        if (!jwtUtils.parseJwt(token).getId().equals(userId)) {
            return ResultResponse.failure(ResultCode.UNAUTHORIZED, "Authorization does not match user");
        }
        // 获取该用户的资源配置
        UserResource userResource = userResourceService.queryUserResourceByUserId(userId);
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
        userResourceService.updateUserResource(userResource);
        return ResultResponse.success();
    }


    /**
     * 之后可以改为AOP获取cookie来检查用户登录信息
     * @param userId
     * @return
     */
    private Boolean checkUserExist(String userId) {
        return userService.queryUserInfo(userId) != null;
    }

}
