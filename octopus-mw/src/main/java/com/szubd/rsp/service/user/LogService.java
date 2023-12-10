package com.szubd.rsp.service.user;

import com.szubd.rsp.pojo.userPojo.LoginLog;

import java.util.List;

public interface LogService {
    Boolean userLoginSuccessLog(String userId);
    Boolean userLoginErrorLog(String userId, String errComment);
    List<LoginLog> queryLoginLog(String userId);
}
