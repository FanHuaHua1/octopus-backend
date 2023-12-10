package com.szubd.rsp.pojo.userPojo;

import lombok.Data;

import java.io.Serializable;

@Data
public class UserInfo implements Serializable {
    private Long id;
    private String userId;
    private String userName;
    private String userPassword;
    private String email;
    private String wxId;
    private String userPrivilege;
    private String token;

    @Override
    public String toString() {
        StringBuilder bd = new StringBuilder();
        bd.append("UserInfo{userId=").append(userId)
                .append(",userName=").append(userName)
                .append(",email=").append(email)
                .append(",wxId=").append(wxId)
                .append(",userPrivileges=").append(userPrivilege)
                .append(",token=").append(token);
        return bd.toString();
    }

    public UserInfo update(UserInfo existUser) {
        if (!this.userId.equals(existUser.getUserId())) {
            return this;
        }
        if (validateName(existUser.userName)) {
            this.setUserName(existUser.userName);
        }
        if (validatePassword(existUser.userPassword)) {
            this.setUserPassword(existUser.userPassword);
        }
        if (validatePrivileges(existUser.userPrivilege)) {
            this.setUserPrivilege(existUser.userPrivilege);
        }
        return this;
    }

    private boolean validateName(String userName) {
        return userName != null && !userName.isEmpty();
    }

    private boolean validatePassword(String userPassword) {
        return userPassword != null && !userPassword.isEmpty();
    }

    private boolean validatePrivileges(String userPrivileges) {
        return userPrivileges != null && !userPrivileges.isEmpty();
    }


}
