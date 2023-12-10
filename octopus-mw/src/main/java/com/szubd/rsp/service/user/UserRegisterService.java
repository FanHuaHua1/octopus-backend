package com.szubd.rsp.service.user;

import com.szubd.rsp.pojo.userPojo.RegisterUser;
import com.szubd.rsp.pojo.userPojo.UserInfo;

public interface UserRegisterService {
    UserInfo registerUser(RegisterUser registerUser);

}
