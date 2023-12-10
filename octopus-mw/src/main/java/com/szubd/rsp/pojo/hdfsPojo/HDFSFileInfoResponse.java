package com.szubd.rsp.pojo.hdfsPojo;

import lombok.Data;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.Serializable;

/**
 * 脱敏HDFS文件信息
 */
@Data
public class HDFSFileInfoResponse implements Serializable {
    private String fileName;
//    private String userPermission;
//    private String groupPermission;
//    private String otherPermission;
    // 转成前端需要的格式
    private String permission;

    private String owner;
    private String group;
    private Long size;
    private Long blockSize;
    private String modificationTime;
    private Short replication;

    public HDFSFileInfoResponse(HDFSFileInfo fileInfo) {
        this.fileName = fileInfo.getFileName();
        // 第一个字符是判断目录，此处是文件-
        this.permission = "-" + fileInfo.getUserPermission() + fileInfo.getGroupPermission() + fileInfo.getOtherPermission();
        this.owner = fileInfo.getOwner();
        this.group = fileInfo.getGroup();
        this.size = fileInfo.getSize();
        this.blockSize = fileInfo.getBlockSize();
        this.modificationTime = fileInfo.getModificationTime();
        this.replication = fileInfo.getReplication();
    }

    @Override
    public String toString() {
        return "HDFSFileInfoResponse{fileName=" + fileName +
                ", permission=" + permission +
                ", owner=" + owner +
                ", group=" + group +
                ", size=" + size +
                ", blockSize=" + blockSize +
                ", modificationTime=" + modificationTime +
                ", replication=" + replication +
                "}";
    }

}
