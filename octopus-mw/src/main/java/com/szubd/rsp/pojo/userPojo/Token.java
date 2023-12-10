package com.szubd.rsp.pojo.userPojo;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class Token {
    private String id;
    private String userId;
    private String token;
    private LocalDateTime createdTime;
    private LocalDateTime expiredTime;
}
