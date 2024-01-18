package com.szubd.rsp.exception;

public class EmailException extends BaseException{
    public EmailException(Integer exceptionCode, String exceptionInfo) {
        super(exceptionCode, exceptionInfo);
    }
}
