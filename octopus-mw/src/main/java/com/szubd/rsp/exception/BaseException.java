package com.szubd.rsp.exception;

public class BaseException extends RuntimeException {

    private Integer exceptionCode;
    private String exceptionInfo;
    private String message;

    public BaseException(Integer exceptionCode, String exceptionInfo) {
        this.exceptionCode = exceptionCode;
        this.exceptionInfo = exceptionInfo;
        this.message = exceptionInfo;
    }

    @Override
    public String getMessage() {
        return message;
    }

    public void setExceptionCode(Integer code) {
        this.exceptionCode = code;
    }
    public Integer getExceptionCode() {
        return this.exceptionCode;
    }

    public String getExceptionInfo() {
        return this.exceptionInfo;
    }


    @Override
    public String toString() {
        return "Exception{code = " + exceptionCode +
                ", info = " + exceptionInfo +
                "}";
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
