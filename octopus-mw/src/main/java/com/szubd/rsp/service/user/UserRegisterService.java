package com.szubd.rsp.service.user;

import com.szubd.rsp.user.RegisterUser;
import com.szubd.rsp.user.UserInfo;
import com.szubd.rsp.user.UserRegisterDubboService;
import org.apache.dubbo.config.annotation.DubboService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@DubboService
@Component
public class UserRegisterService implements UserRegisterDubboService {
    @Autowired
    private UserInfoMapper userInfoMapper;
    private final Logger logger = LoggerFactory.getLogger(UserRegisterService.class);

    public UserInfo registerUser(RegisterUser registerUser) {
        UserInfo existUser = userInfoMapper.queryUserByUserId(registerUser.getUserId());
        // 检查已存在用户
        if (existUser != null) {
            // 用户已存在，返回空值
            logger.info("Register User" +
                    "[userId=" + registerUser.getUserId() +
                    ", userName=" + registerUser.getUserName() + "] is exist");
            return null;
        }
        Long insertId = userInfoMapper.registerUser(registerUser);
        registerUser.setId(insertId);
        return registerUser;
    }
}
