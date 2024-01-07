package com.szubd.rsp.utils;

import com.szubd.rsp.exception.BaseException;
import com.szubd.rsp.exception.ExceptionEnum;
import com.szubd.rsp.exception.HDFSException;
import com.szubd.rsp.user.SerializableFile;
import com.szubd.rsp.user.hdfsPojo.HDFSFileDir;
import com.szubd.rsp.user.hdfsPojo.HDFSFileInfo;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.apache.commons.io.FileUtils.ONE_MB;

@Component
public class HDFSClient {
    private static final Logger logger = LoggerFactory.getLogger(HDFSClient.class);
    private String hdfsUserName;
    @Value("${hdfs.nameNode.url}")
    private String nameNodeIP;

    // ---------------------- Build --------------------------------


    public HDFSClient hdfsUserName(String userName) {
        this.hdfsUserName = userName;
        return this;
    }

    public HDFSClient nameNodeIP(String nameNodeIP) {
        this.nameNodeIP = nameNodeIP;
        return this;
    }


    /**
     * 获取HDFS配置信息 配置文件优先级
     * Configuration  > resource下的hdfs-site.xml > 服务器上的 hdfs-default.xml
     *
     * @return
     */
    private Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("dfs.support.append", "true");
        configuration.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        return configuration;
    }

    private FileSystem getFileSystem() {
        FileSystem fs;
        try {
            fs = FileSystem.get(new URI(this.nameNodeIP), getConfiguration(), this.hdfsUserName);
        } catch (IOException | InterruptedException | URISyntaxException e) {
            throw ExceptionEnum.exception(ExceptionEnum.HDFS_FS_GET_ERR);
        }
        return fs;
    }

    @Override
    public String toString() {
        return "HDFSClient[" +
                "nameNodeIP = " + nameNodeIP +
                ", hdfsUser = " + hdfsUserName +
                "]";
    }


    // ---------------------- Method ---------------------------------


    /**
     * 获取用户目录下的文件和文件夹
     * @param dirPath 完整路径
     * @return
     * @throws IOException
     */
    public HDFSFileDir getHDFSFilesInDir(String dirPath) throws HDFSException {
        FileSystem fs = getFileSystem();

        HDFSFileDir curDir;
        try {
            curDir = new HDFSFileDir(fs.getFileStatus(new Path(dirPath)));

            // 获取该目录下（不含子目录）的所有文件（乱序）
            FileStatus[] fileStatuses = fs.listStatus(new Path(dirPath));
            for (FileStatus hdfsFile : fileStatuses) {
                if (hdfsFile.isDirectory()) {
                    curDir.addDir(new HDFSFileDir(hdfsFile));
                }
                if (hdfsFile.isFile()) {
                    curDir.addFile(new HDFSFileInfo(hdfsFile));
                }
            }

        } catch (IOException e) {
            throw ExceptionEnum.exception(ExceptionEnum.HDFS_FILE_NOT_FOUND);
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    throw ExceptionEnum.exception(ExceptionEnum.HDFS_FS_CLOSE_ERR);
                }
            }
        }

        return curDir;
    }

    /**
     * 在HDFS创建文件夹
     *
     * @param path
     * @return
     * @throws Exception
     */
    public boolean mkdir(String path) {
        FileSystem fs = null;
        boolean isOk = false;
        if (StringUtils.isEmpty(path)) {
            return false;
        }
        try {
            // 路径已存在目录或文件
            if (existFile(path)) {
                logger.info("HDFS mkdir exist {}", path);
                return true;
            }
            // 目标路径
            fs = getFileSystem();
            Path srcPath = new Path(path);
            isOk = fs.mkdirs(srcPath);
            logger.info("HDFS mkdir success: {}", path);
        } catch (IOException e) {
            logger.error("hdfs mkdir exception", e);
            throw ExceptionEnum.exception(ExceptionEnum.HDFS_IO_ERR, "mkdir : Path=" + path);
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    throw ExceptionEnum.exception(ExceptionEnum.HDFS_FS_CLOSE_ERR);
                }
            }
        }
        return isOk;
    }

    /**
     * 判断HDFS文件是否存在
     *
     * @param path
     * @return
     * @throws Exception
     */
    public boolean existFile(String path) {
        Boolean isExists = false;
        FileSystem fs = null;
        if (StringUtils.isEmpty(path)) {
            throw ExceptionEnum.exception(ExceptionEnum.ERR_PARAMETERS, "Null Path");
        }
        try {
            fs = getFileSystem();
            Path srcPath = new Path(path);
            isExists = fs.exists(srcPath);
        } catch (IOException e) {
            throw ExceptionEnum.exception(ExceptionEnum.HDFS_IO_ERR, path.substring(path.indexOf("/", 6)));
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    throw ExceptionEnum.exception(ExceptionEnum.HDFS_FS_CLOSE_ERR);
                }
            }
        }
        return isExists;
    }

//    /**
//     * 读取HDFS目录信息
//     *
//     * @param path
//     * @return
//     * @throws Exception
//     */
//    public List<Map<String, Object>> readPathInfo(String path) throws Exception {
//        try {
//            if (StringUtils.isEmpty(path)) {
//                return null;
//            }
//            if (!existFile(path)) {
//                return null;
//            }
//            FileSystem fs = getFileSystem();
//            // 目标路径
//            Path newPath = new Path(path);
//            FileStatus[] statusList = fs.listStatus(newPath);
//            List<Map<String, Object>> list = new ArrayList<>();
//            if (null != statusList && statusList.length > 0) {
//                for (FileStatus fileStatus : statusList) {
//                    Map<String, Object> map = new HashMap<>();
//                    map.put("filePath", fileStatus.getPath());
//                    map.put("fileStatus", fileStatus.toString());
//                    list.add(map);
//                }
//                return list;
//            }
//        } catch (Exception e) {
//            logger.error("hdfs readPathInfo exception", e);
//        }
//        return null;
//    }

//    /**
//     * HDFS创建文件
//     *
//     * @param path 上传的路径
//     * @param file
//     * @throws Exception
//     */
//    public void createFile(String path, MultipartFile file) throws Exception {
//        if (StringUtils.isEmpty(path) || null == file.getBytes()) {
//            return;
//        }
//        FileSystem fs = null;
//        FSDataOutputStream outputStream = null;
//        try {
//            fs = getFileSystem();
//            String fileName = file.getOriginalFilename();
//            // 上传时默认当前目录，后面自动拼接文件的目录
//            Path newPath = new Path(path + "/" + fileName);
//            // 打开一个输出流
//            outputStream = fs.create(newPath);
//            outputStream.write(file.getBytes());
//            outputStream.flush();
//        } catch (Exception e) {
//            throw e;
//        } finally {
//            if (outputStream != null) {
//                outputStream.close();
//            }
//
//            if (fs != null) {
//                fs.close();
//            }
//        }
//    }

//    /**
//     * 直接往输出流输出文件
//     *
//     * @param path 活动方式 远程文件
//     * @param os   输出流
//     * @return
//     * @throws Exception
//     */
//    public void writeOutputStreamFile(OutputStream os, String path) {
//        if (StringUtils.isEmpty(path)) {
//            return;
//        }
///*        if (!existFile(path)) {
//            // 文件不存在则抛出异常
//            throw new Exception(path + " hdfs文件不存在");
//        }*/
//        FileSystem fs = null;
//        FSDataInputStream inputStream = null;
//        try {
//            // 目标路径
//            Path srcPath = new Path(path);
//            fs = getFileSystem();
//            inputStream = fs.open(srcPath);
//            // 防止中文乱码
//            // BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
//            fileDownload(os, new BufferedInputStream(inputStream));
//        } catch (IOException e) {
//            throw ExceptionEnum.exception(ExceptionEnum.HDFS_IO_ERR, "");
//        } finally {
//            if (inputStream != null) {
//                inputStream.close();
//            }
//            if (fs != null) {
//                fs.close();
//            }
//        }
//    }

    /**
     * 读取HDFS文件内容（使用字符流）
     *
     * @param path
     * @return
     * @throws Exception
     */
    public String readFile(String path) {
        // 检查路径
        if (StringUtils.isEmpty(path)) {
            throw ExceptionEnum.exception(ExceptionEnum.ERR_PARAMETERS);
        }
        // 检查文件是否存在
        if (!existFile(path)) {
            throw ExceptionEnum.exception(ExceptionEnum.HDFS_FILE_NOT_FOUND, "readFile " + path);
        }
        FileSystem fs = null;
        FSDataInputStream inputStream = null;
        try {
            // 获取文件输入流，准备获取数据
            fs = getFileSystem();
            try {
                inputStream = fs.open(new Path(path));
            } catch (IOException e) {
                // 捕获HDFS文件系统抛出的异常，并抛出自定义异常
                throw ExceptionEnum.exception(ExceptionEnum.HDFS_FILE_NOT_FOUND);
            }

            // 以utf8编码读取hdfs文件输入流
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            String lineTxt;
            StringBuffer sb = new StringBuffer();
            while ((lineTxt = reader.readLine()) != null) {
                // 一行一行读
                sb.append(lineTxt);
                sb.append("\n");
            }
            // 去除最后一个换行符
            sb.deleteCharAt(sb.length()-1);
            return sb.toString();
        } catch (IOException e) {
            throw ExceptionEnum.exception(ExceptionEnum.HDFS_IO_ERR);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    throw ExceptionEnum.exception(ExceptionEnum.HDFS_INPUT_STREAM_CLOSE_ERR);
                }
            }
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    throw ExceptionEnum.exception(ExceptionEnum.HDFS_FS_CLOSE_ERR);
                }
            }
        }
    }


    /**
     * 读取HDFS路径下所有文件列表（不含文件夹目录）
     *
     * @param path
     * @return
     * @throws Exception
     */
    public List<Map<String, String>> listFile(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        if (!existFile(path)) {
            return null;
        }

        FileSystem fs = null;
        try {
            fs = getFileSystem();
            // 目标路径
            Path srcPath = new Path(path);
            // 递归找到所有文件(不含文件夹)
            RemoteIterator<LocatedFileStatus> filesList = fs.listFiles(srcPath, true);
            List<Map<String, String>> returnList = new ArrayList<>();
            // 遍历迭代器
            while (filesList.hasNext()) {
                // 获取文件状态
                LocatedFileStatus next = filesList.next();
                String fileName = next.getPath().getName();
                Path filePath = next.getPath();
                // 记录文件的信息
                Map<String, String> map = new HashMap<>();
                map.put("fileName", fileName);
                map.put("filePath", filePath.toString());
                returnList.add(map);
            }
            return returnList;
        } catch (Exception e) {
            logger.error("hdfs listFile {}", e);
        } finally {
            if (fs != null) {
                fs.close();

            }
        }
        return null;
    }


    /**
     * HDFS重命名文件
     *
     * @param oldName
     * @param newName
     * @return
     * @throws Exception
     */
    public boolean renameFile(String oldName, String newName) throws Exception {
        if (StringUtils.isEmpty(oldName) || StringUtils.isEmpty(newName)) {
            return false;
        }
        FileSystem fs = null;
        Boolean isOk = false;
        try {
            fs = getFileSystem();
            // 原文件目标路径
            Path oldPath = new Path(oldName);
            // 重命名目标路径
            Path newPath = new Path(newName);
            isOk = fs.rename(oldPath, newPath);

            return isOk;
        } catch (Exception e) {
            logger.error("hdfs renameFile {}", e);
        } finally {
            if (fs != null) {
                fs.close();
            }
        }
        return isOk;
    }


    /**
     * 删除HDFS文件
     *
     * @param path
     * @return
     * @throws Exception
     */
    public boolean deleteFile(String path) {
        if (StringUtils.isEmpty(path)) {
            return false;
        }

        FileSystem fs = null;
        Boolean isOk = false;
        try {
            if (!existFile(path)) {
                throw ExceptionEnum.exception(ExceptionEnum.HDFS_FILE_NOT_FOUND, path.substring(path.indexOf("/", 6)));
            }
            fs = getFileSystem();
            Path srcPath = new Path(path);
            isOk = fs.deleteOnExit(srcPath);
        } catch (IOException e) {
            throw ExceptionEnum.exception(ExceptionEnum.HDFS_FILE_NOT_FOUND);
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    throw ExceptionEnum.exception(ExceptionEnum.HDFS_FS_CLOSE_ERR);
                }
            }
        }
        return isOk;
    }

    /**
     * 上传HDFS文件
     *
     * @param path       上传路径(本服务器文件全路径)
     * @param uploadPath 目标路径(全节点路径)
     * @throws Exception
     */
    public void uploadFile(String path, String uploadPath) throws Exception {
        if (StringUtils.isEmpty(path) || StringUtils.isEmpty(uploadPath)) {
            return;
        }
        FileSystem fs = null;
        try {
            fs = getFileSystem();
            // 上传路径
            Path clientPath = new Path(path);
            // 目标路径
            Path serverPath = new Path(uploadPath);
            // 调用文件系统的文件复制方法，第一个参数是否删除原文件true为删除，默认为false
            fs.copyFromLocalFile(false, clientPath, serverPath);
        } catch (Exception e) {
            logger.error("hdfs uploadFile {}", e);
        } finally {
            if (fs != null) {
                fs.close();
            }
        }

    }


    /**
     * 下载HDFS文件
     *
     * @param path         hdfs目标路径
     * @param downloadPath 客户端存放路径
     * @throws Exception
     */
    public void downloadFile(String path, String downloadPath) throws Exception {
        if (StringUtils.isEmpty(path) || StringUtils.isEmpty(downloadPath)) {
            return;
        }
        FileSystem fs = null;
        try {
            fs = getFileSystem();
            // hdfs目标路径
            Path clientPath = new Path(path);
            // 客户端存放路径
            Path serverPath = new Path(downloadPath);
            // 调用文件系统的文件复制方法，第一个参数是否删除原文件true为删除，默认为false
            fs.copyToLocalFile(false, clientPath, serverPath);
        } catch (Exception e) {
            logger.error("hdfs downloadFile {}", e);
        } finally {
            if (fs != null) {
                fs.close();
            }
        }
    }


    /**
     * @param os  response输出流
     * @param bis 输入流
     */
    private void fileDownload(OutputStream os, BufferedInputStream bis) throws IOException {
        if (bis == null) {
            return;
        }
        try {
            byte[] buff = new byte[1024];
            int i = bis.read(buff);
            while (i != -1) {
                os.write(buff, 0, i);
                os.flush();
                i = bis.read(buff);
            }
        } catch (IOException e) {
            throw e;
        } finally {
            if (bis != null) {
                try {
                    bis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public Boolean uploadUserFileBytes(String path, SerializableFile file) {
        FileSystem fs = getFileSystem();
        FSDataOutputStream fsDataOutputStream = null;
        try {
            logger.info("Query HDFS FileStatus {}", path);
            try {
                FileStatus fileStatus = fs.getFileStatus(new Path(path));
                // 路径存在文件（目录）
                if (fileStatus != null) {
                    // 如果路径是目录，则上传至目录中 /test_dir -> /test_dir/test_dir
                    if (fileStatus.isDirectory()) {
                        path = path + path.substring(path.lastIndexOf("/"));
                    }
                    // 如果路径是文件，则删除文件重新上传（覆盖）
                    else if (fileStatus.isFile()) {
                        deleteFile(path);
                    }
                }
            } catch (FileNotFoundException e) {
                // 文件不存在,可以创建文件
            } catch (IOException e) {
                throw ExceptionEnum.exception(ExceptionEnum.HDFS_IO_ERR);
            }

            // 创建HDFS文件输出流，输出到HDFS
            logger.info("Create HDFS DataOutputStream {}", path);
            fsDataOutputStream = fs.create(new Path(path));
            // 获取可序列化文件对象的数据
            byte [] bytes = file.getData();
            // 输出流写入数据
            fsDataOutputStream.write(bytes);
            // 输出流推送数据
            fsDataOutputStream.flush();
            // 文件写入完成即返回成功
            if (fs.exists(new Path(path))) {
                return true;
            }
        } catch (IOException e) {
            throw ExceptionEnum.exception(ExceptionEnum.HDFS_OUTPUT_STREAM_GET_ERR);
        } finally {
            try {
                if (fsDataOutputStream != null) {
                    fsDataOutputStream.close();
                }
            } catch (IOException e) {
                throw ExceptionEnum.exception(ExceptionEnum.HDFS_OUTPUT_STREAM_CLOSE_ERR);
            }
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                throw ExceptionEnum.exception(ExceptionEnum.HDFS_FS_CLOSE_ERR);
            }
        }
        return false;
    }

    /**
     * 查看该目录已占用大小
     * @param path
     */
    public Long queryConsumedSpace(String path) {
        FileSystem fs = getFileSystem();
        try {
            ContentSummary summary = fs.getContentSummary(new Path(path));
            long consumedSpace = summary.getSpaceConsumed();
            return consumedSpace;
        } catch (IOException e) {
            throw ExceptionEnum.exception(ExceptionEnum.HDFS_IO_ERR, "queryConsumedSpace(" + path + ")");
        } finally {
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                throw ExceptionEnum.exception(ExceptionEnum.HDFS_FS_CLOSE_ERR);
            }
        }
    }


    /**
     * 移动文件
     * @param filePath
     * @param srcPath
     * @return
     */
    public Boolean moveFile(String filePath, String srcPath) {
        FileSystem fs = getFileSystem();
        try {
            return fs.rename(new Path(filePath), new Path(srcPath));
        } catch (IOException e) {
            throw ExceptionEnum.exception(ExceptionEnum.HDFS_IO_ERR, "Move File from " + filePath + " to " + srcPath);
        } finally {
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                throw ExceptionEnum.exception(ExceptionEnum.HDFS_FS_CLOSE_ERR);
            }
        }
    }

    public byte[] readFileByte(String filePath) {
        FileSystem fs = getFileSystem();
        try {
            if (!fs.exists(new Path(filePath))) {
                throw ExceptionEnum.exception(ExceptionEnum.HDFS_FILE_NOT_FOUND);
            }
            FileStatus fileStatus = fs.getFileStatus(new Path(filePath));
            if (fileStatus.getLen() > 10 * ONE_MB) {
                // 暂时不处理大于10MB的文件
                return null;
            }
            FSDataInputStream inputStream = fs.open(new Path(filePath));

            byte[] resByte = IOUtils.toByteArray(inputStream);
            return resByte;
        } catch (IOException e) {
            throw ExceptionEnum.exception(ExceptionEnum.HDFS_IO_ERR);
        }
    }
}
