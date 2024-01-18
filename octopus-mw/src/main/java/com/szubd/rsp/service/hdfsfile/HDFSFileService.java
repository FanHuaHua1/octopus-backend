package com.szubd.rsp.service.hdfsfile;

import com.szubd.rsp.exception.BaseException;
import com.szubd.rsp.exception.HDFSException;
import com.szubd.rsp.user.SerializableFile;
import com.szubd.rsp.user.hdfsPojo.HDFSFileDir;
import com.szubd.rsp.user.hdfsPojo.HDFSModel;
import com.szubd.rsp.user.hdfsPojo.HDFSModelType;

import java.io.IOException;
import java.util.List;

public interface HDFSFileService {

    /**
     * 创建HDFS用户目录
     * @param user 新用户
     * @return
     */
    Boolean createHDFSUser(String user) throws HDFSException;

    /**
     * 创建用户目录
     * @param user 执行用户
     * @param dirPath 用户目录路径
     * @return
     */
    Boolean createHDFSDir(String user, String dirPath) throws HDFSException;

    /**
     * 删除HDFS目录（已被deleteHDFSFile取代）
     * @param user 执行用户
     * @param dirPath 用户目录路径
     * @return
     */
    @Deprecated
    Boolean deleteHDFSDir(String user, String dirPath) throws HDFSException;

    /**
     * 上传HDFS文件（暂时放在这里，可能要部署到另一个服务中）
     * @param user 上传用户
     * @param file 上传的用户目录文件路径
     * @return
     */
    Boolean uploadHDFSFile(String user, String path, SerializableFile file) throws HDFSException;

    /**
     * 删除用户文件
     * @param user 执行用户
     * @param filePath 删除的用户目录文件路径
     * @return
     */
    Boolean deleteHDFSFile(String user, String filePath) throws HDFSException;

    /**
     * 获取目录中的文件和文件夹
     * @param user
     * @param filePath
     * @return
     * @throws HDFSException
     */
    HDFSFileDir getHDFSFilesInDir(String user, String filePath) throws HDFSException;

    List<HDFSModelType> getModelAllType(String user, String filePath) throws HDFSException;

    List<HDFSModel> getModelList(String user, String filePath)  throws HDFSException;

    @Deprecated
    String downloadFileString(String user, String filePath);

    /**
     * 获取文件字节流
     * @param user
     * @param filePath
     * @return
     * @throws HDFSException
     */
    byte[] downloadFileByte(String user, String filePath) throws HDFSException;

    /**
     * 获取用户所有的占用空间
     * @param user
     * @return
     */
    Long getAllConsumedSpace(String user);

    /**
     * 获取用户目录路径下的占用空间
     * @param user
     * @param path
     * @return
     * @throws HDFSException
     */
    Long getConsumedSpace(String user, String path) throws HDFSException;

    /**
     * 移动剪切用户的文件
     * @param user
     * @param filePath
     * @param srcPath
     * @return
     * @throws HDFSException
     */
    Boolean moveFile(String user, String filePath, String srcPath) throws HDFSException;


}
