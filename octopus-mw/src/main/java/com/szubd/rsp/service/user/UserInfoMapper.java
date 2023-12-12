package com.szubd.rsp.service.user;

import com.szubd.rsp.user.RegisterUser;
import com.szubd.rsp.user.UserInfo;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

@Mapper
@Repository
public interface UserInfoMapper {
    UserInfo queryUserByUserId(String userId);

    Boolean checkUserPassword(String userId, String userPassword);

    Long registerUser(RegisterUser user);

    Boolean updateUser(UserInfo user);

}
