package com.szubd.rsp.service.user;

import com.szubd.rsp.user.UserResource;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface UserResourceMapper {
    UserResource queryUserResourceByUserId(String userId);

    Boolean insertNewUserResource(UserResource userResource);

    Boolean updateByUserId(UserResource userResource);
}
