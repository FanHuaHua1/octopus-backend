package com.szubd.rsp.service.hdfsfile;

import com.szubd.rsp.exception.HDFSException;
import com.szubd.rsp.user.SerializableFile;
import com.szubd.rsp.user.hdfsPojo.HDFSFileDir;
import com.szubd.rsp.user.hdfsPojo.HDFSFileInfo;
import com.szubd.rsp.user.hdfsPojo.HDFSModel;
import com.szubd.rsp.user.hdfsPojo.HDFSModelType;
import com.szubd.rsp.utils.HDFSClient;
import org.apache.dubbo.config.annotation.DubboService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@DubboService
@Component
public class HDFSFileServiceImpl implements HDFSFileService {
    @Value("${hdfs.nameNode.url}")
    private String HDFS_IP;

    @Override
    public Boolean createHDFSUser(String user) throws HDFSException {
        HDFSClient client = new HDFSClient()
                .hdfsUserName(user)
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
}
