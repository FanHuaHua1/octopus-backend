package com.szubd.rsp.user;

public interface UserLoginDubboService {

    public UserInfo queryUserByUserId(String userId);

    public Boolean checkUserPassword(String userId, String password);

}
