package com.szubd.rsp.service.hdfsfile;

import com.szubd.rsp.LocalFile.UploadResultDto;
import com.szubd.rsp.exception.BusinessException;
import com.szubd.rsp.exception.HDFSException;
import com.szubd.rsp.user.SerializableFile;
import com.szubd.rsp.user.hdfsPojo.*;
import com.szubd.rsp.utils.HDFSClient;
import com.szubd.rsp.utils.StringTools;
import org.apache.commons.io.FileUtils;
import org.apache.dubbo.config.annotation.DubboService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

@DubboService
@Component
public class HDFSFileServiceImpl implements HDFSFileService {
    @Value("${hdfs.nameNode.url}")
    private String HDFS_IP;
    @Autowired
    @Lazy
    private HDFSFileServiceImpl hdfsFileService;
    private static final Logger logger = LoggerFactory.getLogger(HDFSFileServiceImpl.class);

    @Override
    public Boolean createHDFSUser(String user) throws HDFSException {
        HDFSClient client = new HDFSClient()
                .hdfsUserName("hdfs")
                .nameNodeIP(HDFS_IP);
        // 组装用户文件夹路径
        String dirPath = "/user/" + user;
        String algoPath = dirPath + "/algo";
        String modelPath = dirPath + "/model";
        String dataPath = dirPath + "/dataset";
        return client.mkdir(algoPath) && client.mkdir(modelPath) && client.mkdir(dataPath);
    }

    @Override
    public Boolean createHDFSDir(String user, String dirPath) throws HDFSException {
        HDFSClient client = new HDFSClient()
                .hdfsUserName(user)
                .nameNodeIP(HDFS_IP);
        // 组装HDFS路径
        dirPath = dirPath.trim();
        dirPath = concateUserDir(user, dirPath);
        return client.mkdir(dirPath);
    }

    @Override
    public HDFSFileDir getHDFSFilesInDir(String user, String path) throws HDFSException {
        System.out.println(path + " =======================");
        HDFSClient client = new HDFSClient()
                .hdfsUserName(user)
                .nameNodeIP(HDFS_IP);
        path = concateUserDir(user, path);
        HDFSFileDir hdfsFileDir = client.getHDFSFilesInDir(path);
        return hdfsFileDir;
    }
    // 查询所有模型的类别
    @Override
    public List<HDFSModelType> getModelAllType(String user, String path) throws HDFSException {

        HDFSClient client = new HDFSClient()
                .hdfsUserName(user)
                .nameNodeIP(HDFS_IP);
        System.out.println("请求路径是:" + path);
        path = concateUserDir(user, path);
        HDFSFileDir hdfsFilesInDir = client.getHDFSFilesInDir(path);
        System.out.println(hdfsFilesInDir.toString());
        //进行包装
        List<HDFSFileDir> dirs = hdfsFilesInDir.getDirs();
//        List<HDFSFileInfo> files = hdfsFilesInDir.getFiles();
        List<HDFSModelType> modelTypes = new ArrayList<>();
        for (HDFSFileDir dir : dirs) {
            String[] split = dir.getRelativePath().split("/");
            try{
                List<Map<String, String>> maps = client.listFile(dir.getCurPath());
                int size = maps.size();
                modelTypes.add(new HDFSModelType(split[split.length - 1], size));
            }catch (Exception e){
                throw new RuntimeException(e);
            }

        }
        return modelTypes;

    }

    @Override
    public List<HDFSModel> getModelList(String user, String path) throws HDFSException {
        HDFSClient client = new HDFSClient()
                .hdfsUserName(user)
                .nameNodeIP(HDFS_IP);
//        System.out.println("请求路径是:" + filePath);
        path = concateUserDir(user, path);
        HDFSFileDir hdfsFilesInDir = client.getHDFSFilesInDir(path);
        System.out.println(hdfsFilesInDir.toString());
        List<HDFSModel> hdfsModels = new ArrayList<>();
        List<HDFSFileInfo> files = hdfsFilesInDir.getFiles();
        for (HDFSFileInfo fileInfo : files) {
            String[] split = path.split("/");
            String modelType = split[split.length-1];
            hdfsModels.add(new HDFSModel(fileInfo.getFileName(), modelType, Long.toString(fileInfo.getSize())));
        }
        return hdfsModels;
    }

    @Override
    public Boolean deleteHDFSDir(String user, String dirPath) throws HDFSException {
        HDFSClient client = new HDFSClient()
                .hdfsUserName(user)
                .nameNodeIP(HDFS_IP);
        dirPath = concateUserDir(user, dirPath);
        return client.deleteFile(dirPath);
    }

    @Override
    public Boolean uploadHDFSFile(String user, String path, SerializableFile file) throws HDFSException {
        HDFSClient client = new HDFSClient()
                .hdfsUserName(user)
                .nameNodeIP(HDFS_IP);
        path = concateUserDir(user, path);
        // 创建前置目录
        boolean isMkdir = client.mkdir(path.substring(0, path.lastIndexOf("/")));
        if (!isMkdir) return false;
        // 上传文件数据
        return client.uploadUserFileBytes(path, file);
    }

    @Override
    public Boolean deleteHDFSFile(String user, String filePath) {
        HDFSClient client = new HDFSClient()
                .hdfsUserName(user)
                .nameNodeIP(HDFS_IP);
        filePath = concateUserDir(user, filePath);

        Boolean isDelete;
        try {
            isDelete = client.deleteFile(filePath);
        } catch (HDFSException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return isDelete;
    }

    private String concateUserDir(String user, String path) {
//        if (path.startsWith("/")) return "/user/" + user + "/" + "dataset" + path;
//        return "/user/" + user + "/" + "dataset" + path;
        if (path.startsWith("/")) return "/user/" + user + "/"  + path;
        return "/user/" + user + "/"  + path;
//        "/user/" + user + "/" + "dataset" + path;
    }

    @Override
    public String downloadFileString(String user, String filePath) {
        HDFSClient client = new HDFSClient()
                .hdfsUserName(user)
                .nameNodeIP(HDFS_IP);

        filePath = concateUserDir(user, filePath);
        return client.readFile(filePath);
    }

    @Override
    public byte[] downloadFileByte(String user, String filePath) throws HDFSException {
        HDFSClient client = new HDFSClient()
                .hdfsUserName(user)
                .nameNodeIP(HDFS_IP);
        filePath = concateUserDir(user, filePath);
        byte[] fileData = client.readFileByte(filePath);
        return fileData;
    }


    @Override
    public Long getAllConsumedSpace(String user) {
        return getConsumedSpace(user, "/");
    }

    @Override
    public Long getConsumedSpace(String user, String path) throws HDFSException {
        HDFSClient client = new HDFSClient()
                .hdfsUserName(user)
                .nameNodeIP(HDFS_IP);
        path = concateUserDir(user, path);
        return client.queryConsumedSpace(path);
    }

    @Override
    public Boolean moveFile(String user, String filePath, String srcPath) throws HDFSException {
        HDFSClient client = new HDFSClient()
                .hdfsUserName(user)
                .nameNodeIP(HDFS_IP);
        filePath = concateUserDir(user, filePath);
        srcPath = concateUserDir(user, srcPath);

        Boolean ifMove = false;
        try {
            ifMove = client.moveFile(filePath, srcPath);
        } catch (HDFSException e) {
            throw e;
        }

        return ifMove;
    }

    @Override
    public UploadResultDto uploadFile(String username, String path, String fileId, MultipartFile file, String fileName, Integer chunkIndex, Integer chunks) {
        File tempFileFolder = null;
        UploadResultDto resultDto = new UploadResultDto();
        Boolean uploadSuccess = true;
        try {
            if(StringTools.isEmpty(fileId)) {
                fileId = StringTools.getRandomString(10);
            }
            resultDto.setFileId(fileId);
            Date curDate = new Date();
            //算空间

            //
            //临时目录
            String tempFolderName = "D:/temp/users/" + username + "/";
            //文件夹名字
            String currentUserFolderName = username + "_" + fileId;
            //创建临时目录
            tempFileFolder = new File(tempFolderName + currentUserFolderName);
            if (!tempFileFolder.exists()) {
                tempFileFolder.mkdirs();
            }
            //判断磁盘空间

            //

            //将文件暂存在本地文件夹
            File newFile = new File(tempFileFolder.getPath() + "/" + chunkIndex);
            file.transferTo(newFile);
//            resultDto.setStatus("uploading");
            //不是最后一个分片，直接返回
            if(chunkIndex < chunks - 1) {
                resultDto.setStatus("uploading");
                return resultDto;
            }
            logger.info("该文件有{}", getFolderSize(tempFileFolder));
            //限制用户上传太多文件

            //
            //最后一个分片上传完成，异步合成分片，合成完毕后发送到HDFS
            String fileSuffix = StringTools.getFileSuffix(fileName);
            String realFileName = currentUserFolderName + fileSuffix;
            transferFile(fileId, path, username, tempFileFolder, fileName);
            resultDto.setStatus("upload_finish");

        } catch (IOException e) {
            uploadSuccess = false;
            logger.error("文件上传失败", e);
        } catch (Exception e) {
            uploadSuccess = false;
            logger.error("文件上传失败", e);
            throw new RuntimeException(e);
        } finally {
            //如果上传失败，清除临时目录
            if (tempFileFolder != null && !uploadSuccess) {
                try {
                    FileUtils.deleteDirectory(tempFileFolder);
                } catch (IOException e) {
                    logger.error("删除临时目录失败");
                }
            }
        }
        return resultDto;
    }

    private static long getFolderSize(File file) {
        if (file == null || !file.exists()) {
            return 0L;
        } else if (!file.isDirectory()) {
            return file.length();
        } else {
            long totalSize = 0L;

            for (File child : file.listFiles()) {
                totalSize += getFolderSize(child);
            }

            return totalSize;
        }
    }
    public void transferFile(String fileId, String path, String username, File tempFileFolder, String fileName) throws Exception {
        Boolean transferSuccess = true;
        String targetFilePath = null;
        //目标目录
        String targetFolderName = "D:/targetFile/users/" + username + "/";
        File targetFolder = new File(targetFolderName + fileId);
        if (!targetFolder.exists()) {
            targetFolder.mkdirs();
        }
        String realFileName = username + "_" + fileId + "_" + fileName;
        targetFilePath = targetFolder.getPath() + "/" + realFileName;
        union(tempFileFolder.getPath(), targetFilePath, realFileName, true);
        //发送到hdfs
        HDFSClient client = new HDFSClient()
                .hdfsUserName(username)
                .nameNodeIP(HDFS_IP);
        // 组装用户文件夹路径
        String dirPath = "/user/" + username;
        String abPath = dirPath + path;
        logger.info("当前文件路径为{}", abPath);
        client.uploadFile(targetFilePath, abPath);
        //删除本地文件
        try {
            if(targetFolder != null) FileUtils.deleteDirectory(targetFolder);
        } catch (IOException e) {
            logger.error("删除本地目录失败");
        }


    }
    public static void union(String dirPath, String toFilePath, String fileName, boolean delSource) throws BusinessException {
        File dir = new File(dirPath);
        if (!dir.exists()) {
            throw new BusinessException(11111, "目录不存在");
        }
        File fileList[] = dir.listFiles();
        File targetFile = new File(toFilePath);
        RandomAccessFile writeFile = null;
        try {
            writeFile = new RandomAccessFile(targetFile, "rw");
            byte[] b = new byte[1024 * 10];
            for (int i = 0; i < fileList.length; i++) {
                int len = -1;
                //创建读块文件的对象
                File chunkFile = new File(dirPath + File.separator + i);
                RandomAccessFile readFile = null;
                try {
                    readFile = new RandomAccessFile(chunkFile, "r");
                    while ((len = readFile.read(b)) != -1) {
                        writeFile.write(b, 0, len);
                    }
                } catch (Exception e) {
                    logger.error("合并分片失败", e);
                    throw new BusinessException(11111, "合并文件失败");
                } finally {
                    readFile.close();
                }
            }
        } catch (Exception e) {
            logger.error("合并文件:{}失败", fileName, e);
            throw new BusinessException(11111, "合并文件失败");
        } finally {
            try {
                if (null != writeFile) {
                    writeFile.close();
                }
            } catch (IOException e) {
                logger.error("关闭流失败", e);
            }
            if (delSource) {
                if (dir.exists()) {
                    try {
                        FileUtils.deleteDirectory(dir);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }


}
