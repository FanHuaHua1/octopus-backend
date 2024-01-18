package com.szubd.rsp.service.user;

import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.sun.istack.internal.NotNull;
import com.szubd.rsp.http.Result;
import com.szubd.rsp.http.ResultCode;
import com.szubd.rsp.http.ResultResponse;
import com.szubd.rsp.user.*;
import com.szubd.rsp.utils.Constant;
import com.szubd.rsp.utils.CreateImageCode;
import com.szubd.rsp.utils.JwtUtils;
import com.szubd.rsp.utils.PageParam;
import io.jsonwebtoken.Claims;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.dubbo.config.annotation.DubboReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * 用户信息接口，包含：
 * 用户身份注册
 * 用户身份登录
 * 用户信息查询
 * 用户身份更新
 *
 * @author leonardo
 */
@Controller
@RequestMapping("/user")
public class UserInfoController {
    //    @Autowired
    @DubboReference
    private UserRegisterDubboService userRegisterDubboService;

    @Autowired
    private UserService userService;

    //    @Autowired
    @DubboReference
    private UserResourceDubboService userResourceDubboService;

    //    @Autowired
    @DubboReference
    private LogDubboService logDubboService;


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

    //判断token是否有效
    @ResponseBody
    @RequestMapping("/queryTokenValid")
    public Result queryTokenValid(HttpServletRequest request) {
//        String userId = getUserIdFromRequest(request);
        return ResultResponse.success();
    }

    //验证码
    @RequestMapping(value = "/checkCode")
    public void checkCode(HttpServletResponse response, HttpSession session) throws
            IOException {
        CreateImageCode vCode = new CreateImageCode(130, 38, 5, 10);
        response.setHeader("Pragma", "no-cache");
        response.setHeader("Cache-Control", "no-cache");
        response.setDateHeader("Expires", 0);
        response.setContentType("image/jpeg");
        String code = vCode.getCode();
        session.setAttribute(Constant.CHECK_CODE_KEY_EMAIL, code);
        vCode.write(response.getOutputStream());

    }
    @ResponseBody
    @RequestMapping("/setEmail")
    public Result setEmail(HttpSession session, @RequestBody JSONObject jsonObject) {
        String userId = jsonObject.getString("userId");
        String password = jsonObject.getString("password");
        String emailCode = jsonObject.getString("emailCode");
        String checkCode = jsonObject.getString("checkCode");
        String emailNew = jsonObject.getString("emailNew");
        if (!Objects.equals(session.getAttribute(Constant.EMAIL_CODE).toString(), emailCode.toLowerCase())) {
            return ResultResponse.failure(ResultCode.EMAIL_INVALID);
        }
        if(!Objects.equals(session.getAttribute(Constant.CHECK_CODE_KEY_EMAIL).toString(), checkCode.toLowerCase())){
            return ResultResponse.failure(ResultCode.Picture_CODE_INVALID);
        }
        //查了两次数据库，可以优化
        UserInfo userInfo = userService.queryUserInfo(userId);
        if(userInfo == null) {
            return ResultResponse.failure(ResultCode.USERID_NOEXIST);
        }
        if(!userService.checkUserPassword(userId, password)) {
            return ResultResponse.failure(ResultCode.PASSWORD_ERROR);
        }
        //经过前面校验，此时可以重设邮箱了
        userService.updateEmail(userId, emailNew);
        session.removeAttribute(Constant.EMAIL_CODE);
        return ResultResponse.success();
    }

    //根据userId和图形验证码发送邮箱验证码
    @ResponseBody
    @RequestMapping("/sendEmailCode")
    public Result sendEmailCode(HttpSession session, @RequestBody JSONObject jsonObject) {
        try {
            String userId = jsonObject.getString("userId");
            String checkCode = jsonObject.getString("checkCode");
            String emailNew = jsonObject.getString("emailNew");
            System.out.println(session.getAttribute(Constant.CHECK_CODE_KEY_EMAIL).toString() + " 和 " + checkCode);
            if (!Objects.equals(session.getAttribute(Constant.CHECK_CODE_KEY_EMAIL).toString(), checkCode.toLowerCase())) {
                return ResultResponse.failure(ResultCode.Picture_CODE_INVALID);
            }
            if(emailNew != null) {
                //发邮件
                String code = RandomStringUtils.random(5, false, true);
                userService.sendEmailCode(emailNew, code);
                session.setAttribute(Constant.EMAIL_CODE, code);
                logger.info("绑定邮箱：生成的邮箱认证码为 {}", code);
                return ResultResponse.success();
            }
            UserInfo userInfo = userService.queryUserInfo(userId);
            if(userInfo == null) {
                return ResultResponse.failure(ResultCode.USERID_NOEXIST);
            }
            //从数据库中查邮箱
            String email = userInfo.getEmail();
            if(email == null || email.length() <= 2) {
                return ResultResponse.failure(ResultCode.EMAIL_NOEXIST);
            }
            String code = RandomStringUtils.random(5, false, true);
            System.out.println(email + " 和 " + code);
            userService.sendEmailCode(email, code);
            session.setAttribute(Constant.EMAIL_CODE, code);
            logger.info("生成的邮箱认证码为 {}", code);
            return ResultResponse.success();
        }
        finally {
            session.removeAttribute(Constant.CHECK_CODE_KEY_EMAIL);
        }
    }

    @ResponseBody
    @RequestMapping("/resetPwd")
    public Result resetPwd(HttpSession session, @RequestBody JSONObject jsonObject) {
        String userId = jsonObject.getString("userId");
        String code = jsonObject.getString("code");
        String userPassword = jsonObject.getString("userPassword");
        String reUserPassword = jsonObject.getString("reUserPassword");
        if (!Objects.equals(session.getAttribute(Constant.EMAIL_CODE).toString(), code.toLowerCase())) {
            return ResultResponse.failure(ResultCode.EMAIL_INVALID);
        }
        if(!Objects.equals(userPassword, reUserPassword)) {
            return ResultResponse.failure(ResultCode.PWD_CHECK_TWICE_INVALID);
        }
        userService.updateUserPassword(userId, userPassword);
        session.removeAttribute(Constant.EMAIL_CODE);
        return ResultResponse.success();
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
            logDubboService.userLoginErrorLog(userId, "无此用户或密码错误");
            return ResultResponse.failure(ResultCode.UNAUTHORIZED, "无此用户或密码错误");
        }
    }

    @ResponseBody
    @RequestMapping("/registerUser")
    public Result register(@RequestBody RegisterUser registerUser) {
        logger.info("Register User: " + registerUser.toString());
        // 注册用户
        UserInfo user = userRegisterDubboService.registerUser(registerUser);
        // 如果已有用户将返回空
        if (user == null) return ResultResponse.failure(ResultCode.PARAMS_IS_INVALID, "该用户ID已被注册");

        // 注册用户起始配额
        Boolean initResource = userResourceDubboService.initUserResource(user.getUserId());
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

//    /**
//     * 更新用户信息，更新的用户从request中获取
//     *
//     * @param userInfo 更新的用户信息
//     * @param request
//     * @return
//     */
//    @ResponseBody
//    @RequestMapping("/updateUser")
//    public Result updateUser(@RequestBody UserInfo userInfo, HttpServletRequest request) {
//        logger.info("Update User: " + userInfo.toString());
//        String userId = getUserIdFromRequest(request);
//        userInfo.setUserId(userId);
//        UserInfo updateUser = userService.updateUser(userInfo);
//        return ResultResponse.success(updateUser);
//    }

    private Claims getClaimsFromToken(String token) {
        return jwtUtils.parseJwt(token);
    }

    /**
     * 从TOKEN中获取用户ID
     *
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
     *
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
     *
     * @param request
     * @param key     用户信息key
     * @return
     */
    private Object getUserInfoFromRequest(HttpServletRequest request, String key) {
        String token = request.getHeader("Authorization");
        token = token.replace("Bearer ", "");
        return getClaimsFromToken(token).getOrDefault(key, null);
    }

    @ResponseBody
    @RequestMapping("/listUser")
    public Result listUser(UserInfo userInfo) {
        logger.info("get User List " );
        List<UserInfo> list = userService.getUserList(userInfo);
        return ResultResponse.success(list);
    }
    @ResponseBody
    @RequestMapping("/id")
    public Result getUserById(Long id) {
        System.out.println(id);
        UserInfo userInfo = userService.getUserById(id);
        return ResultResponse.success(userInfo);
    }
    @ResponseBody
    @RequestMapping("/pageUser")
    public Result pageUser(PageParam page, UserInfo userInfo) {
//        logger.info("get User List " );
        Page<UserInfo> userInfoPage = new Page<>(page.getCurrent(), page.getSize());
//        System.out.println("——————————分页结果："+page.getSize()+"————————");
        Page<UserInfo> list = userService.getUserPage(userInfoPage, userInfo);
        return ResultResponse.success(list);
    }
    /**
     * 更新用户信息，更新的用户从request中获取
     * @param userInfo 更新的用户信息
     * @return
     */
    @ResponseBody
    @RequestMapping("/updateUser")
    public Result updateUser(@RequestBody UserInfo userInfo) {
        logger.info("Update User: " + userInfo.toString());
//        String userId = userInfo.getUserId();
//        System.out.println(userId);
//        userInfo.setUserId(userId);
        UserInfo updateUser = userService.updateUser(userInfo);
        return ResultResponse.success(updateUser);
    }

    @ResponseBody
    @RequestMapping("/delUser")
    public Result delUser(String ids) {
        String[] split = ids.split(",");
        for (String element : split) {
            userService.delUser(Long.valueOf(element));
        }
//        userInfo.setUserId(userId);
//        UserInfo updateUser = userDubboService.updateUser(userInfo);
        return ResultResponse.success();
    }


}
