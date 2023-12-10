package com.szubd.rsp.service.impl;

import com.szubd.rsp.mapper.UserInfoMapper;
import com.szubd.rsp.pojo.userPojo.RegisterUser;
import com.szubd.rsp.pojo.userPojo.UserInfo;
import com.szubd.rsp.service.user.UserRegisterService;
import org.apache.dubbo.config.annotation.DubboService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@DubboService
@Component
public class UserRegisterServiceImpl implements UserRegisterService {
    @Autowired
    private UserInfoMapper userInfoMapper;
    private final Logger logger = LoggerFactory.getLogger(UserRegisterServiceImpl.class);

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
