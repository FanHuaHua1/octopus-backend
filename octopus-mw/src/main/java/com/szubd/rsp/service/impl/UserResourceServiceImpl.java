package com.szubd.rsp.service.impl;

import com.szubd.rsp.mapper.UserInfoMapper;
import com.szubd.rsp.mapper.UserResourceMapper;
import com.szubd.rsp.pojo.resourcePojo.UserResource;
import com.szubd.rsp.service.user.UserResourceService;
import org.apache.dubbo.config.annotation.DubboService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@DubboService
@Component
public class UserResourceServiceImpl implements UserResourceService {
    @Autowired
    private UserInfoMapper userMapper;
    @Autowired
    private UserResourceMapper userResourceMapper;
    public UserResource queryUserResourceByUserId(String userId) {
        // 无此用户
        if (userMapper.queryUserByUserId(userId) == null) return null;
        return userResourceMapper.queryUserResourceByUserId(userId);
    }

    public Boolean initUserResource(String userId) {
        UserResource userResource = new UserResource(userId);
        return userResourceMapper.insertNewUserResource(userResource);
    }

    public Boolean updateUserResource(UserResource userResource) {
        String userId = userResource.getUserId();
        UserResource existUserResource = this.queryUserResourceByUserId(userId);
        existUserResource.update(userResource);
        return userResourceMapper.updateByUserId(userResource);
    }
}
