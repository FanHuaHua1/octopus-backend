package com.szubd.rsp.exception;

/**
 * 记录不同异常，并包装获取异常类的方法
 */
public enum ExceptionEnum {
    // 用户Token系列异常
    NULL_TOKEN(new TokenException(4001, "Authorization Failed - Null Authorization")),
    EXPIRED_TOKEN(new TokenException(4002, "Authorization Failed - Expired Authorization")),
    ERR_USER_TOKEN(new TokenException(4003, "Authorization Failed - Error Authorization")),
    NOT_LOGIN_TOKEN(new TokenException(4004, "Authorization Failed - Not Login Token")),

    ERR_PARAMETERS(new BaseException(4005, "Parameters Error : %s")),
    SYSTEM_ERR(new BaseException(500, "Unknown Error")),


    // HDFS文件系统异常
    HDFS_FILE_NOT_FOUND(new HDFSException(7001, "HDFS File Not Found : Path=%s")),
    HDFS_FS_CLOSE_ERR(new HDFSException(7002, "Failed to Close HDFS FileSystem")),
    HDFS_FS_GET_ERR(new HDFSException(7003, "Failed to Get HDFS FileSystem")),


    HDFS_OUTPUT_STREAM_CLOSE_ERR(new HDFSException(7004, "Failed to Close HDFS DataOutputStream")),
    HDFS_OUTPUT_STREAM_GET_ERR(new HDFSException(7005, "Failed to Get HDFS DataOutputStream")),
    HDFS_IO_ERR(new HDFSException(7006, "HDFS IOException Error %s")),
    HDFS_FILE_EXISTED(new HDFSException(7007, "HDFS File Exist : Path=%s")),
    HDFS_INPUT_STREAM_CLOSE_ERR(new HDFSException(7008, "Failed to Close HDFS DataIntputStream")),;


    private final BaseException exception;

    ExceptionEnum(BaseException e) {
        this.exception = e;
    }

    /**
     * 根据枚举类获取可抛出的异常
     * @param exceptionEnum
     * @return
     */

    public static BaseException exception(ExceptionEnum exceptionEnum) {
        return exceptionEnum.exception;
    }

    public static BaseException exception(Integer code, String info) {
        return new BaseException(code, info);
    }

    /**
     * 将一些异常需要展示的信息传入异常信息中
     * 利用String.format("Info=%s", obj)
     * @param exceptionEnum
     * @param objs
     * @return
     */
    public static BaseException exception(ExceptionEnum exceptionEnum, Object... objs) {
        exceptionEnum.exception.setExceptionInfo(String.format(exceptionEnum.exception.getExceptionInfo(), objs));
        return exceptionEnum.exception;
    }

    public static BaseException exception(Integer code) {
        return new BaseException(code, null);
    }

    @Override
    public String toString() {
        return "ExceptionEnum{" + this.exception.toString() + "}";
    }
}
