package com.szubd.rsp.mapper;

import com.szubd.rsp.pojo.userPojo.Token;
import org.apache.ibatis.annotations.Mapper;

import java.time.LocalDateTime;

@Mapper
public interface TokenMapper {

    Boolean insertToken(String userId, String token, LocalDateTime insertTime, LocalDateTime expiredTime);

    Token queryTokenByUserId(String userId);

//    Integer deleteTokenByExpiredTime(LocalDateTime expiredTime);

    Integer deleteExpiredToken();

    Integer deleteTokenByUserId(String userId);

    Integer refreshExpiredTimeByUserId(String userId);
}
