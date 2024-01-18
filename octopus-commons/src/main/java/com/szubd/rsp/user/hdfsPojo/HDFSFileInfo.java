package com.szubd.rsp.user.hdfsPojo;

import lombok.Data;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Time;

import java.io.Serializable;

/**
 * 记录文件信息（与目录区分），需要将HDFS的FileStatus转换过来
 */
@Data
public class HDFSFileInfo implements Serializable {
    private String fileName;
    private String fullPath;
//    private HDFSFileDir belongDir;
    private FsPermission permission;
    // 对应ugo权限
    private String userPermission;
    private String groupPermission;
    private String otherPermission;

    private String owner;
    private String group;
    private Long size;
    private Long blockSize;
    private Long modificationTimeStamp;
    private String modificationTime;
    private Short replication;

    public HDFSFileInfo(FileStatus file) {
        if (file == null) return;
        if (file.isFile()) {
            String path = file.getPath().toString();
            this.fullPath = path;

            int fileNameStart = path.lastIndexOf("/");
            this.fileName = path.substring(fileNameStart + 1);

            this.permission = file.getPermission();
            this.userPermission = this.permission.getUserAction().SYMBOL;
            this.groupPermission = this.permission.getGroupAction().SYMBOL;
            this.otherPermission = this.permission.getOtherAction().SYMBOL;

            this.owner = file.getOwner();
            this.group = file.getGroup();
            this.size = file.getLen();
            this.blockSize = file.getBlockSize();
            this.modificationTimeStamp = file.getModificationTime();
            this.modificationTime = Time.formatTime(this.modificationTimeStamp);
            this.replication = file.getReplication();
        }
    }

//    public HDFSFileInfo(FileStatus file, HDFSFileDir parent) {
//        this(file);
//        if (parent == null) return;
//        this.belongDir = parent;
//    }



    @Override
    public String toString() {
        return "HDFSFileInfo{fileName=" + fileName +
                ", fullPath=" + fullPath +
//                ", dir=" + belongDir.getRelativePath() +
                ", permission=" + permission +
                ", permissionStr=" + userPermission + groupPermission + otherPermission +
                ", owner=" + owner +
                ", group=" + group +
                ", size=" + size +
                ", blockSize=" + blockSize +
                ", modificationTime=" + modificationTime +
                ", replication=" + replication +
                "}";
    }

}
