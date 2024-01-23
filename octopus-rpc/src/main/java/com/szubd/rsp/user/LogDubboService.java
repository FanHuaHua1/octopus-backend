package com.szubd.rsp.user;


import java.util.List;

public interface LogDubboService {
    Boolean userLoginSuccessLog(String userId);
    Boolean userLoginErrorLog(String userId, String errComment);
    List<LoginLog> queryLoginLog(String userId);
}
