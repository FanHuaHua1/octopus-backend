package com.szubd.rsp.service.hdfsfile;

import com.szubd.rsp.exception.ExceptionEnum;
import com.szubd.rsp.exception.HDFSException;
import com.szubd.rsp.http.Result;
import com.szubd.rsp.http.ResultCode;
import com.szubd.rsp.http.ResultResponse;
import com.szubd.rsp.user.SerializableFile;
import com.szubd.rsp.user.hdfsPojo.HDFSFileDir;
import com.szubd.rsp.user.hdfsPojo.HDFSFileDirResponse;
import com.szubd.rsp.user.query.HDFSDeleteFilesQuery;
import com.szubd.rsp.user.query.HDFSMoveFileQuery;
import com.szubd.rsp.service.hdfsfile.HDFSFileService;
import com.szubd.rsp.utils.JwtUtils;
import io.jsonwebtoken.Claims;
import org.apache.dubbo.config.annotation.DubboReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.*;

/**
 * 关于HDFS文件系统的操作接口
 * @author leonardo
 */
@Controller
@RequestMapping("/hdfs")
public class HDFSFileController {
    @Autowired
    private JwtUtils jwtUtils;
    @DubboReference
    private HDFSFileService hdfsService;

    private static final Logger logger = LoggerFactory.getLogger(HDFSFileController.class);

    /**
     * 获取路径下的文件和文件夹信息：
     * 示例：
     * 用户leo，path=/test_dir
     * 则获取HDFS:/user/leo/test_dir下的文件和文件夹
     * @param request
     * @param path
     * @return
     */
    @ResponseBody
    @RequestMapping("/myFiles")
    public Result getFilesInContent(HttpServletRequest request, String path) {
        String user = getHDFSUserFromRequest(request);

        HDFSFileDir hdfsFilesInDir;
        try {
            hdfsFilesInDir = hdfsService.getHDFSFilesInDir(user, path);
        } catch (HDFSException e) {
            throw e;
        }
        return ResultResponse.success(new HDFSFileDirResponse(hdfsFilesInDir));
    }

    /**
     * 删除用户目录下的HDFS文件，用户身份从TOKEN中获取，路径从请求中获取
     * @param request
     * @param path 从用户文件夹开始的HDFS路径，后端组装用户路径
     * @return
     */
    @ResponseBody
    @RequestMapping("/deleteFile")
    public Result deleteFile(HttpServletRequest request, String path) {
        String user = getHDFSUserFromRequest(request);
        Boolean deleteSuccess = hdfsService.deleteHDFSFile(user, path);
        if (deleteSuccess) {
            return ResultResponse.success("Delete Success");
        }
        return ResultResponse.failure(ResultCode.PARAMS_IS_INVALID, "No such file or directory");
    }

    /**
     * 批量删除文件（文件夹）
     * @param request
     * @param query
     * @return
     */
    @ResponseBody
    @RequestMapping("/deleteFileBatch")
    public Result deleteFileBatch(HttpServletRequest request, @RequestBody HDFSDeleteFilesQuery query) {
        String user = getHDFSUserFromRequest(request);
        List<String> path = query.getPaths();
        if (path == null) {
            throw ExceptionEnum.exception(ExceptionEnum.ERR_PARAMETERS, "path is null");
        }
        logger.info("User {} request to delete files {}", user, path);
        Iterator iter = path.iterator();
        List<String> failedFiles = new ArrayList<>();
        while (iter.hasNext()) {
            String filePath = String.valueOf(iter.next());
            try {
                logger.info("Start delete user {} file {}", user, filePath);
                hdfsService.deleteHDFSFile(user, filePath);
            } catch (HDFSException e) {
                String exceptionInfo = e.getMessage();
                failedFiles.add("delete " + filePath + " failed, exception=" + exceptionInfo);
            }
        }
        if (failedFiles.isEmpty()) return ResultResponse.success("Delete Complete Success");
        return ResultResponse.success(failedFiles);
    }

    @ResponseBody
    @RequestMapping("/moveFile")
    public Result moveFile(HttpServletRequest request, @RequestBody HDFSMoveFileQuery query) {
        String user = getHDFSUserFromRequest(request);
        List<String> filesPath = query.getFilesPath();
        String srcPath = query.getSrcPath();
        logger.info("user {} move files {} to {}", user, filesPath, srcPath);

        List<String> exceptionFile = new ArrayList<>();
        try {
            for (String filePath : filesPath) {
                if (!hdfsService.moveFile(user, filePath, srcPath)) {
                    exceptionFile.add("Move file failed: check your path from " + filePath + " to " + srcPath);
                }
            }
        } catch (HDFSException e) {
            exceptionFile.add("Move file err:" + e.getMessage());
        }

        if (exceptionFile.isEmpty()) return ResultResponse.success();
        return ResultResponse.success(exceptionFile);
    }

    /**
     * 创建文件夹
     * @param request
     * @param dirPath
     * @return
     * @throws Exception
     */
    @ResponseBody
    @RequestMapping("/mkdir")
    public Result createHDFSdir(HttpServletRequest request, String dirPath) {
        String user = getHDFSUserFromRequest(request);
        Boolean ifMkdir = hdfsService.createHDFSDir(user, dirPath);
        if (ifMkdir) {
            return ResultResponse.success();
        }
        return ResultResponse.failure(ResultCode.INTERNAL_SERVER_ERROR);
    }


    /**
     * 从请求中获取访问的用户信息，并获取HDFS用户名
     * @param request
     * @return
     */
    private String getHDFSUserFromRequest(HttpServletRequest request) {
        String token = request.getHeader("Authorization");
        token = token.replace("Bearer", "");
        return getHDFSUserFromToken(token);
    }

    /**
     * 从TOKEN中获取用户信息，并将userName作为HDFS用户名
     * @param token
     * @return
     */
    private String getHDFSUserFromToken(String token) {
        Claims claims = jwtUtils.parseJwt(token);

        // 使用userId作为hdfs用户名还是userName作为hdfs用户名
//        String userId = claims.getId();
        String userName = claims.getSubject();

        return userName;
    }

    /**
     * 上传文件（测试时使用的form-data，type=file,Content=*\/*）
     * 上传较小的文件，大文件需要使用分片上传
     * @param request
     * @param path
     * @param file
     * @return
     * @throws Exception
     */
    @ResponseBody
    @RequestMapping("/uploadFile")
    public Result uploadSingleSmallFile(HttpServletRequest request, String path, @RequestBody MultipartFile file) {
        String user = getHDFSUserFromRequest(request);

        logger.info("upload user:" + user + ", upload path:" + path);

        // 将上传来的文件信息转换成可序列化的对象（重要的是数据file.getBytes)
        SerializableFile serializableFile;
        Boolean res = null;
        try {
            serializableFile = new SerializableFile(file);
            res = hdfsService.uploadHDFSFile(user, path, serializableFile);
        } catch (HDFSException e) {
            throw e;
        } catch (IOException e) {
            throw ExceptionEnum.exception(ExceptionEnum.SERIALIZATION_FAILED);
        }

        return ResultResponse.success(res);
    }

    /**
     * 下载文件，但返回字节流还是字符流暂未确定
     * @param request
     * @param path
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/downloadFile", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity downloadFile(HttpServletRequest request, String path) {
        String user = getHDFSUserFromRequest(request);
        logger.info("download user:" + user + ", download path:" + path);

//        String data = hdfsService.downloadFileString(user, path);
        byte[] data;
        try {
            data = hdfsService.downloadFileByte(user, path);
        } catch (HDFSException e) {
            throw e;
        }
        return ResponseEntity.ok().body(data);
//        return ResultResponse.success(data);
    }

    /**
     * 获取用户目录下所有文件的占用空间（已计算replication）
     * @param request
     * @return
     */
    @ResponseBody
    @RequestMapping("/myAllConsumedSpace")
    public Result getAllConsumdeSpace(HttpServletRequest request) {
        String user = getHDFSUserFromRequest(request);
        logger.info("Search occupied space user:" + user);

        Long usedSpace = hdfsService.getAllConsumedSpace(user);
        return ResultResponse.success(usedSpace);
    }

    /**
     * 获取目录下文件占用的空间（已计算replication）
     * @param request
     * @param path
     * @return
     */
    @ResponseBody
    @RequestMapping("/myConsumedSpace")
    public Result getConsumedSpace(HttpServletRequest request, String path) {
        String user = getHDFSUserFromRequest(request);
        logger.info("Search occupied space user:" + user);

        Long used = hdfsService.getConsumedSpace(user, path);
        return ResultResponse.success(used);
    }


}
