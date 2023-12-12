package com.szubd.rsp.exception;

public class TokenException extends BaseException {

    public TokenException(Integer exceptionCode, String exceptionInfo) {
        super(exceptionCode, exceptionInfo);
    }

    @Override
    public String toString() {
        return "TokenException{code=" + getExceptionCode() +
                ", info='" + getExceptionInfo() + "'" +
                "}";
    }
}
