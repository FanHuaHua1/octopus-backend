package com.szubd.rsp.service.user;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.szubd.rsp.user.RegisterUser;
import com.szubd.rsp.user.UserInfo;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

import java.util.List;

@Mapper
@Repository
public interface UserInfoMapper {
    UserInfo queryUserByUserId(String userId);

    Boolean checkUserPassword(String userId, String userPassword);

    String getUserPassword(String userId);

    Long registerUser(RegisterUser user);

    Boolean updateUser(UserInfo user);

    Boolean updatePassword(String userId, String userPassword);

    Boolean updateEmail(String userId, String email);
    List<UserInfo> getUserList(UserInfo user);

    Page<UserInfo> getUserPage(Page<UserInfo> userPage , UserInfo userInfo);

    Boolean delUser(Long id);


}
