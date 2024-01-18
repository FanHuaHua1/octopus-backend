package com.szubd.rsp.user;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;

import javax.servlet.http.HttpSession;
import java.util.List;

public interface UserDubboService {
    UserInfo queryUserInfo(String userId);

    /**
     * 登录用户，调用该接口前检查用户是否已登录，密码是否正确
     * @param userId
     * @return
     */
    UserInfo login(String userId);

    Boolean checkUserPassword(String userId, String userPassword);

    UserInfo updateUser(UserInfo userInfo);

//    Boolean checkToken(String userId, String token);

//    void cleanExpiredToken();

//    boolean checkExistToken(String userId);

    Boolean checkRawToken(String rawToken);

    void sendEmailCode(String email, String toEmail);

    Boolean updateUserPassword(String userId, String password);

    Boolean updateEmail(String userId, String email);

    Page<UserInfo> getUserPage(Page<UserInfo> page, UserInfo userInfo);
    List<UserInfo> getUserList(UserInfo userInfo);
    UserInfo getUserById(Long id);
    Boolean delUser(Long id);
}
