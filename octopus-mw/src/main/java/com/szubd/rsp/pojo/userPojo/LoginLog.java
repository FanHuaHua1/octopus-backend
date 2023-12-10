package com.szubd.rsp.pojo.userPojo;

import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * 登录日志数据格式
 */
@Data
public class LoginLog implements Serializable {
    private Long id;
    private String loginUserId;
    private Timestamp loginTime;
    private String loginStatus;
    private String errComment;

    @Override
    public String toString() {
        return "LoginLog[userId=" + loginUserId +
                ", loginTime=" + loginTime +
                ", status=" + loginStatus +
                ", comment=" + errComment +
                "]";
    }
}
