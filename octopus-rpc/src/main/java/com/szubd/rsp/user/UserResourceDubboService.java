package com.szubd.rsp.user;

public interface UserResourceDubboService {
    UserResource queryUserResourceByUserId(String userId);
    Boolean initUserResource(String userId);
    Boolean updateUserResource(UserResource userResource);
}
