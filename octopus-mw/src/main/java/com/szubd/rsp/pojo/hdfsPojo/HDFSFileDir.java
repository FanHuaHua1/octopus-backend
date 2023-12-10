package com.szubd.rsp.pojo.hdfsPojo;

import lombok.Data;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * HDFS文件结构
 * curDir 当前文件夹路径（全路径）
 */
@Data
public class HDFSFileDir implements Serializable {
    private String curPath;
    /**
     * 截取自/username后的路径
     */
    private String relativePath;

    private long length;
    private Boolean isdir;
    private short block_replication;
    private long blocksize;
    private long modification_time;
    private long access_time;
    private FsPermission permission;
    private String owner;
    private String group;
//    private Path symlink;

//    /**
//     * 父目录，如果已经是根目录，则为空
//     */
//    private HDFSFileDir parentDir;
    /**
     * 该文件夹下的文件
     */
    private List<HDFSFileInfo> files = new ArrayList<>();
    /**
     * 该目录下的其他文件夹
     */
    private List<HDFSFileDir> dirs = new ArrayList<>();

    public HDFSFileDir() {}

    public HDFSFileDir(FileStatus file) throws IOException {
        this();
        if (file.isDirectory()) {
            // 仅目录创建
            String fullPath = file.getPath().toString();
            this.curPath = fullPath;
            // 只取用户目录部分
            int relativePosition = fullPath.indexOf("/user/") + 5;
            this.relativePath = this.curPath.substring(relativePosition);

            this.length = file.getLen();
            this.isdir = file.isDirectory();
            this.block_replication = file.getReplication();
            this.blocksize = file.getBlockSize();
            this.modification_time = file.getModificationTime();
            this.access_time = file.getAccessTime();
            this.permission = file.getPermission();
            this.owner = file.getOwner();
            this.group = file.getGroup();
            // 一旦不是link连接就会报错，暂时也用不上这个
//            this.symlink = file.getSymlink();
        }
    }

//    public HDFSFileDir nextDir(String dirName) {
//        return dirs.stream()
//                .filter(dir -> Objects.equals(dir.relativePath, dirName))
//                .findFirst()
//                .get();
//    }

    public void addFile(HDFSFileInfo file) {
//        file.setBelongDir(this);
        this.files.add(file);
    }

    public void addDir(HDFSFileDir dir) {
//        dir.setParentDir(this);
        this.dirs.add(dir);
    }

    @Override
    public String toString() {
        return "HDFSDir{path=" + curPath +
                ", relativePath=" + relativePath +
                ", files = " + files.toString() +
                ", dirs = " + dirs.toString() +
                "}";
    }
}
