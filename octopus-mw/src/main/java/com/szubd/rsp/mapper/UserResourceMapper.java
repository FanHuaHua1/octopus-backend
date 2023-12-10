package com.szubd.rsp.mapper;

import com.szubd.rsp.pojo.resourcePojo.UserResource;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface UserResourceMapper {
    UserResource queryUserResourceByUserId(String userId);

    Boolean insertNewUserResource(UserResource userResource);

    Boolean updateByUserId(UserResource userResource);
}
