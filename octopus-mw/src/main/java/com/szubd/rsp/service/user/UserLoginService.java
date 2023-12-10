package com.szubd.rsp.service.user;

import com.szubd.rsp.mapper.UserInfoMapper;
import com.szubd.rsp.pojo.userPojo.UserInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class UserLoginService {
    @Autowired
    private UserInfoMapper userMapper;

    public UserInfo queryUserByUserId(String userId) {
        UserInfo user = userMapper.queryUserByUserId(userId);
        return user;
    }

    public Boolean checkUserPassword(String userId, String password) {
        Boolean ifCorrect = userMapper.checkUserPassword(userId, password);
        return ifCorrect;
    }

}
