package com.szubd.rsp.service.user;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.szubd.rsp.exception.EmailException;
import com.szubd.rsp.exception.ExceptionEnum;
import com.szubd.rsp.user.UserDubboService;
import com.szubd.rsp.user.UserInfo;
import com.szubd.rsp.utils.Constant;
import com.szubd.rsp.utils.JwtUtils;
import com.szubd.rsp.utils.SysSettingsDto;
import io.jsonwebtoken.Claims;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.dubbo.config.annotation.DubboService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.mail.internet.MimeMessage;
import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@DubboService
@Component
public class UserService implements UserDubboService {
    @Autowired
    private UserInfoMapper userMapper;
//    private TokenMapper tokenMapper;
    @Autowired
    private LogService logService;
    @Value("${token.expiredHour}")
    private Integer tokenExpiredHour = 1;

    @Autowired
    private JwtUtils jwtUtils;
    @Resource
    private JavaMailSender javaMailSender;
    @Value("${email.sendUserName}")
    private String sendUserName;
    private static final Logger logger = LoggerFactory.getLogger(UserService.class);

    @Override
    public UserInfo queryUserInfo(String userId) {
        return userMapper.queryUserByUserId(userId);
    }

    @Override
    public UserInfo login(String userId) {
        UserInfo user = userMapper.queryUserByUserId(userId);

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
//            tokenMapper.insertToken(user.getUserId(), token, now, exp);
        user.setToken(token);
        logService.userLoginSuccessLog(userId);

        return user;
    }

    @Override
    public Boolean checkUserPassword(String userId, String password) {
        Boolean ifCorrect = userMapper.checkUserPassword(userId, password);
        return ifCorrect;
    }



//    @Override
//    public Boolean checkToken(String userId, String token) {
//        Token existToken = tokenMapper.queryTokenByUserId(userId);
//        if (existToken == null) return false;
//        return existToken.getToken().equals(token);
//    }

//    @Override
//    public void cleanExpiredToken() {
//        tokenMapper.deleteExpiredToken();
//    }

//    @Override
//    public boolean checkExistToken(String userId) {
//        return tokenMapper.queryTokenByUserId(userId) != null;
//    }


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


//        if (!this.checkToken(userId, token)) {
//            // 检查TOKEN是否与数据库一致，不一致则拒绝请求
//            logger.error("Token and userId does not match");
//            throw ExceptionEnum.exception(ExceptionEnum.NOT_LOGIN_TOKEN);
//        }

        return true;
    }
   @Override
    public void sendEmailCode(String toEmail, String code) {
        try {
            MimeMessage message = javaMailSender.createMimeMessage();

            MimeMessageHelper helper = new MimeMessageHelper(message, true);
            //邮件发件人,要配置成公共变量，采取注解形式
            helper.setFrom(sendUserName);
            //邮件收件人 1或多个
            helper.setTo(toEmail);

            SysSettingsDto sysSettingsDto = new SysSettingsDto();

            //邮件主题
            helper.setSubject(sysSettingsDto.getRegisterEmailTitle());
            //邮件内容
            helper.setText(String.format(sysSettingsDto.getRegisterEmailContent(), code));
            //邮件发送时间
            helper.setSentDate(new Date());
            javaMailSender.send(message);
        } catch (Exception e) {
            logger.error("邮件发送失败", e);
            throw new EmailException(5555 , "邮件发送失败");
        }
    }

    @Override
    public Boolean updateUserPassword(String userId, String password) {
        return userMapper.updatePassword(userId, password);
    }

    @Override
    public Boolean updateEmail(String userId, String email) {
        return userMapper.updateEmail(userId, email);
    }

    @Override
    public Page<UserInfo> getUserPage(Page<UserInfo> page, UserInfo userInfo) {

        LambdaQueryWrapper<UserInfo> userLambdaQueryWrapper = Wrappers.lambdaQuery();
        if(userInfo.getUserId()!=null|| !userInfo.getUserId().equals("")){
            userLambdaQueryWrapper.eq(UserInfo::getUserId,userInfo.getUserId());
        }
        if(userInfo.getUserName()!=null|| !userInfo.getUserName().equals("")){
            userLambdaQueryWrapper.like(UserInfo::getUserName,userInfo.getUserName());
        }
        if(userInfo.getUserPrivilege()!=null|| !userInfo.getUserPrivilege().equals("")){
            userLambdaQueryWrapper.like(UserInfo::getUserPrivilege,userInfo.getUserPrivilege());
        }

        return userMapper.getUserPage(page,userInfo);
    }

    @Override
    public List<UserInfo> getUserList(UserInfo userInfo) {
        return userMapper.getUserList(userInfo);
    }

    @Override
    public UserInfo getUserById(Long id) {
        UserInfo userInfo = new UserInfo();
        userInfo.setId(id);

        return userMapper.getUserList(userInfo).get(0);
    }

    @Override
    public UserInfo updateUser(UserInfo userInfo) {
//        String userId = userInfo.getUserId();
//        UserInfo existUser = login(userId);
//        更新用户信息要考虑学号和用户名是否重复
        UserInfo user = new UserInfo();
        user.setUserName(userInfo.getUserName());
        List<UserInfo> userList = getUserList(user);
        if(!userList.isEmpty()){
            return userInfo;
        }
        userMapper.updateUser(userInfo);
        return userInfo;
    }

    @Override
    public Boolean delUser(Long id) {
//        应该先处理hdfs空间的释放问题
        return userMapper.delUser(id);
    }
}
