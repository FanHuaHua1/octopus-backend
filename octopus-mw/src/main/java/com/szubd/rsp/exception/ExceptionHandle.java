package com.szubd.rsp.exception;

import com.szubd.rsp.http.Result;
import com.szubd.rsp.http.ResultCode;
import com.szubd.rsp.http.ResultResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 全局统一异常处理【注意，仅接收Controller中抛出的异常，filter层的异常不会接收】
 * @author leonardo
 *
 */
@RestControllerAdvice
public class ExceptionHandle {
    private static final Logger logger = LoggerFactory.getLogger(ExceptionHandle.class);

    @ExceptionHandler(TokenException.class)
    public Result handle(TokenException e) {
        logger.error("Handle TokenException {}", e.toString());
        Result<String> res = new Result<>();
        res.setCode(e.getExceptionCode());
        res.setMessage(e.getExceptionInfo());
        return res;
    }

    @ExceptionHandler(HDFSException.class)
    public Result handle(HDFSException e) {
        logger.error("Handle HDFSException {}", e.toString());
        Result<String> res = new Result<>();
        res.setCode(e.getExceptionCode());
        res.setMessage(e.getExceptionInfo());
        return res;
    }

    @ExceptionHandler(BaseException.class)
    public Result handle(BaseException e) {
        logger.error("Handle BaseException {}", e.toString());
        Result<String> res = new Result<>();
        res.setCode(e.getExceptionCode());
        res.setMessage(e.getExceptionInfo());
        return res;
    }

}
