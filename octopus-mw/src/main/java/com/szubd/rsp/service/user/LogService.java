package com.szubd.rsp.service.user;

import com.szubd.rsp.user.LogDubboService;
import com.szubd.rsp.user.LoginLog;
import org.apache.dubbo.config.annotation.DubboService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@DubboService
@Component
public class LogService implements LogDubboService {
    @Autowired
    private LogMapper logMapper;

    /**
     * 用户登录成功日志（到时候可以用AOP来切，暂时先用这个）
     * @param userId 登录用户的USER_ID
     * @return 是否记录日志成功
     */
    public Boolean userLoginSuccessLog(String userId) {
        return logMapper.loginSuccessLog(userId);
    }

    public Boolean userLoginErrorLog(String userId, String errComment) {
        return logMapper.loginErrorLog(userId, errComment);
    }

    @Override
    public List<LoginLog> queryLoginLog(String userId) {
        return logMapper.queryByUserId(userId);
    }
}
