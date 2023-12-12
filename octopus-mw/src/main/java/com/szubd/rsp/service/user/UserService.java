package com.szubd.rsp.service.user;

import com.szubd.rsp.exception.ExceptionEnum;
import com.szubd.rsp.user.Token;
import com.szubd.rsp.user.UserDubboService;
import com.szubd.rsp.user.UserInfo;
import com.szubd.rsp.utils.JwtUtils;
import io.jsonwebtoken.Claims;
import org.apache.dubbo.config.annotation.DubboService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@DubboService
@Component
public class UserService implements UserDubboService {
    @Autowired
    private UserInfoMapper userMapper;
    @Autowired
    private TokenMapper tokenMapper;
    @Autowired
    private LogService logService;
    @Value("${token.expiredHour}")
    private Integer tokenExpiredHour = 1;

    @Autowired
    private JwtUtils jwtUtils;
    private static final Logger logger = LoggerFactory.getLogger(UserService.class);

    @Override
    public UserInfo queryUserInfo(String userId) {
        return userMapper.queryUserByUserId(userId);
    }

    @Override
    public UserInfo login(String userId) {
        UserInfo user = userMapper.queryUserByUserId(userId);

        Token existToken = tokenMapper.queryTokenByUserId(userId);
        if (existToken == null) {
            String token;
            Map<String, Object> dataMap = new HashMap<>();
            dataMap.put("userName", user.getUserName());
            dataMap.put("userPrivilege", user.getUserPrivilege());
            dataMap.put("email", user.getEmail());
            dataMap.put("wxId", user.getWxId());
            token = jwtUtils.createJwt(user.getUserId(), user.getUserName(), dataMap);
            LocalDateTime now = LocalDateTime.now();
            LocalDateTime exp = now.plusHours(tokenExpiredHour);
            logger.info("Login User {} at {} to get token and expired at {}", user.getUserId(), now, exp);
            // 3小时过期
            tokenMapper.insertToken(user.getUserId(), token, now, exp);
            user.setToken(token);
        } else {
            logger.info("Login User {} already has token", userId);
            user.setToken(existToken.getToken());
        }
        logService.userLoginSuccessLog(userId);

        return user;
    }

    @Override
    public Boolean checkUserPassword(String userId, String password) {
        Boolean ifCorrect = userMapper.checkUserPassword(userId, password);
        return ifCorrect;
    }

    @Override
    public UserInfo updateUser(UserInfo userInfo) {
        String userId = userInfo.getUserId();
        UserInfo existUser = login(userId);
        existUser.update(existUser);
        userMapper.updateUser(userInfo);
        return existUser;
    }

    @Override
    public Boolean checkToken(String userId, String token) {
        Token existToken = tokenMapper.queryTokenByUserId(userId);
        if (existToken == null) return false;
        return existToken.getToken().equals(token);
    }

    @Override
    public void cleanExpiredToken() {
        tokenMapper.deleteExpiredToken();
    }

    @Override
    public boolean checkExistToken(String userId) {
        return tokenMapper.queryTokenByUserId(userId) != null;
    }


    @Override
    public Boolean checkRawToken(String rawToken) {
        // 其他请求需要检查用户TOKEN
        if (rawToken == null || rawToken.isEmpty()) {
            logger.error("Null Authorization");
            throw ExceptionEnum.exception(ExceptionEnum.NULL_TOKEN);
        }
        // 一般情况前端会在前面加上Bearer，目前前端没加，但不影响
        String token = rawToken.replace("Bearer ", "");

        //解析token
        //获取clamis（其他数据）
        Claims claims = jwtUtils.parseJwt(token);
        if (claims == null) {
            // 过期
            logger.error("Receive expired Token");
            throw ExceptionEnum.exception(ExceptionEnum.EXPIRED_TOKEN);
        }

        String userId = claims.getId();
        if (userId == null) {
            logger.error("Error Authorization");
            throw ExceptionEnum.exception(ExceptionEnum.ERR_USER_TOKEN);
        }

        if (!this.checkToken(userId, token)) {
            // 检查TOKEN是否与数据库一致，不一致则拒绝请求
            logger.error("Token and userId does not match");
            throw ExceptionEnum.exception(ExceptionEnum.NOT_LOGIN_TOKEN);
        }

        return true;
    }
}
