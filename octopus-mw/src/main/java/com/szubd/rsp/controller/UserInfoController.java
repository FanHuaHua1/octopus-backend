package com.szubd.rsp.controller;

import com.sun.istack.internal.NotNull;
import com.szubd.rsp.http.Result;
import com.szubd.rsp.http.ResultCode;
import com.szubd.rsp.http.ResultResponse;
import com.szubd.rsp.pojo.userPojo.RegisterUser;
import com.szubd.rsp.pojo.userPojo.UserInfo;
import com.szubd.rsp.service.user.*;
import com.szubd.rsp.utils.JwtUtils;
import io.jsonwebtoken.Claims;
import org.apache.dubbo.config.annotation.DubboReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

/**
 * 用户信息接口，包含：
 * 用户身份注册
 * 用户身份登录
 * 用户信息查询
 * 用户身份更新
 * @author leonardo
 */
@Controller
@RequestMapping("/user")
public class UserInfoController {
//    @Autowired
    @DubboReference
    private UserRegisterService userRegisterService;

//    @Autowired
    @DubboReference
    private UserService userService;

//    @Autowired
    @DubboReference
    private UserResourceService userResourceService;

//    @Autowired
    @DubboReference
    private LogService logService;

//    @DubboReference
//    private HDFSService hdfsService;

    @Autowired
    private JwtUtils jwtUtils;

    private static final Logger logger = LoggerFactory.getLogger(UserInfoController.class);



    @ResponseBody
    @RequestMapping("/queryUser")
    public Result queryUser(HttpServletRequest request) {
        String userId = getUserIdFromRequest(request);
        logger.info("Query user whose userId = " + userId);
        UserInfo user = userService.queryUserInfo(userId);
        String token = request.getHeader("Authorization").replace("Bearer", "");
        Claims claims = jwtUtils.parseJwt(token);
        if (claims == null || !claims.getId().equals(userId)) return ResultResponse.failure(ResultCode.UNAUTHORIZED);
        return ResultResponse.success(user);
    }

    @ResponseBody
    @RequestMapping("/loginUser")
    public Result loginUser(@NotNull @RequestBody UserInfo userInfo) {
        String userId = userInfo.getUserId();
        String password = userInfo.getUserPassword();
        logger.info("Login User: userId=" + userId + ", password=" + password);
        Boolean ifCorrect = userService.checkUserPassword(userId, password);
        if (ifCorrect) {
            UserInfo user = userService.login(userId);
            return ResultResponse.success(user);
        } else {
            logService.userLoginErrorLog(userId, "无此用户或密码错误");
            return ResultResponse.failure(ResultCode.UNAUTHORIZED, "无此用户或密码错误");
        }
    }

    @ResponseBody
    @RequestMapping("/registerUser")
    public Result register(@RequestBody RegisterUser registerUser) {
        logger.info("Register User: " + registerUser.toString());
        // 注册用户
        UserInfo user = userRegisterService.registerUser(registerUser);
        // 如果已有用户将返回空
        if (user == null) return ResultResponse.failure(ResultCode.PARAMS_IS_INVALID, "该用户ID已被注册");

        // 注册用户起始配额
        Boolean initResource = userResourceService.initUserResource(user.getUserId());
        if (!initResource) return ResultResponse.failure(ResultCode.INTERNAL_SERVER_ERROR);

        // 注册用户HDFS空间
        try {
//            initResource = hdfsService.createHDFSUser(user.getUserName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (!initResource) return ResultResponse.failure(ResultCode.INTERNAL_SERVER_ERROR);

        return ResultResponse.success(user);
    }

    /**
     * 更新用户信息，更新的用户从request中获取
     * @param userInfo 更新的用户信息
     * @param request
     * @return
     */
    @ResponseBody
    @RequestMapping("/updateUser")
    public Result updateUser(@RequestBody UserInfo userInfo, HttpServletRequest request) {
        logger.info("Update User: " + userInfo.toString());
        String userId = getUserIdFromRequest(request);
        userInfo.setUserId(userId);
        UserInfo updateUser = userService.updateUser(userInfo);
        return ResultResponse.success(updateUser);
    }

    private Claims getClaimsFromToken(String token) {
        return jwtUtils.parseJwt(token);
    }

    /**
     * 从TOKEN中获取用户ID
     * @param request
     * @return
     */
    private String getUserIdFromRequest(HttpServletRequest request) {
        String token = request.getHeader("Authorization");
        token = token.replace("Bearer ", "");
        return getClaimsFromToken(token).getId();
    }

    /**
     * 从TOKEN中获取用户名
     * @param request
     * @return
     */
    private String getUserNameFromRequest(HttpServletRequest request) {
        String token = request.getHeader("Authorization");
        token = token.replace("Bearer ", "");
        return getClaimsFromToken(token).getSubject();
    }

    /**
     * 根据key获取TOKEN中包装的其他用户信息
     * @param request
     * @param key 用户信息key
     * @return
     */
    private Object getUserInfoFromRequest(HttpServletRequest request, String key) {
        String token = request.getHeader("Authorization");
        token = token.replace("Bearer ", "");
        return getClaimsFromToken(token).getOrDefault(key, null);
    }


}
