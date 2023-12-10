package com.szubd.rsp.mapper;

import com.szubd.rsp.pojo.userPojo.RegisterUser;
import com.szubd.rsp.pojo.userPojo.UserInfo;
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
