package com.szubd.rsp.exception;

public class BusinessException extends BaseException{
    public BusinessException(Integer exceptionCode, String exceptionInfo) {
        super(exceptionCode, exceptionInfo);
    }
}
