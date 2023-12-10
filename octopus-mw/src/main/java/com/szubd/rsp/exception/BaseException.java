package com.szubd.rsp.exception;

public class BaseException extends RuntimeException {

    private Integer exceptionCode;
    private String exceptionInfo;

    public BaseException(Integer exceptionCode, String exceptionInfo) {
        this.exceptionCode = exceptionCode;
        this.exceptionInfo = exceptionInfo;
    }

    @Override
    public String getMessage() {
        return exceptionInfo;
    }

    public void setExceptionCode(Integer code) {
        this.exceptionCode = code;
    }
    public Integer getExceptionCode() {
        return exceptionCode;
    }

    public void setExceptionInfo(String info) {
        this.exceptionInfo = info;
    }
    public String getExceptionInfo() {
        return exceptionInfo;
    }


    @Override
    public String toString() {
        return "Exception{code = " + exceptionCode +
                ", info = " + exceptionInfo +
                "}";
    }
}
