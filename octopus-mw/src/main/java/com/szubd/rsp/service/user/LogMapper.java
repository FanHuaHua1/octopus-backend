package com.szubd.rsp.service.user;

import com.szubd.rsp.user.LoginLog;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

import java.util.List;

@Mapper
@Repository
public interface LogMapper {
    Boolean loginSuccessLog(String userId);
    Boolean loginErrorLog(String userId, String errComment);

    List<LoginLog> queryByUserId(String userId);
}
