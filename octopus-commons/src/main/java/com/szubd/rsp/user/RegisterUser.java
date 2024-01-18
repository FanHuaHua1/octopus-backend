package com.szubd.rsp.user;

import lombok.Data;

import java.io.Serializable;

@Data
public class RegisterUser extends UserInfo implements Serializable {
    private Long id;
    private String userId;
    private String userName;
    private String userPassword;
    private String email;
    private String wxId;
    // 默认普通用户
    private String userPrivilege = "2";

    @Override
    public String toString() {
        return "RegisterUser :" +
                " userId=" + userId +
                ", name=" + userName +
                ", password=" + userPassword +
                ", email=" + email +
                ", wxId=" + wxId +
                ", userPrivilege=" + userPrivilege;
    }
}
