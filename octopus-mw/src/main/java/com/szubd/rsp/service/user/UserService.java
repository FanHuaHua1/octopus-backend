package com.szubd.rsp.service.user;

import com.szubd.rsp.exception.TokenException;
import com.szubd.rsp.pojo.userPojo.UserInfo;

public interface UserService {
    UserInfo queryUserInfo(String userId);

    /**
     * 登录用户，调用该接口前检查用户是否已登录，密码是否正确
     * @param userId
     * @return
     */
    UserInfo login(String userId);

    Boolean checkUserPassword(String userId, String userPassword);

    UserInfo updateUser(UserInfo userInfo);

    Boolean checkToken(String userId, String token);

    void cleanExpiredToken();

    boolean checkExistToken(String userId);

    Boolean checkRawToken(String rawToken) throws TokenException;
}
