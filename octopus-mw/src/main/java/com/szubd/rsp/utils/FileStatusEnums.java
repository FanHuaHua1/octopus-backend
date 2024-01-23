package com.szubd.rsp.utils;


public enum FileStatusEnums {
    TRANSFER(0, "同步中"),
    TRANSFER_FAIL(1, "同步失败"),
    USING(2, "使用中");

    private Integer status;
    private String desc;

    FileStatusEnums(Integer status, String desc) {
        this.status = status;
        this.desc = desc;
    }

    public Integer getStatus() {
        return status;
    }

    public String getDesc() {
        return desc;
    }
}
