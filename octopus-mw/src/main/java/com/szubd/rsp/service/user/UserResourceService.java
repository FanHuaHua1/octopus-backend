package com.szubd.rsp.service.user;

import com.szubd.rsp.pojo.resourcePojo.UserResource;

public interface UserResourceService {
    UserResource queryUserResourceByUserId(String userId);
    Boolean initUserResource(String userId);
    Boolean updateUserResource(UserResource userResource);
}
