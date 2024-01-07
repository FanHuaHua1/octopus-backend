package com.szubd.rsp.user.hdfsPojo;

import lombok.Data;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Time;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 脱敏后给用户的信息
 */
@Data
public class HDFSFileDirResponse implements Serializable {
    /**
     * 截取自/username后的路径
     */
    private String relativePath;
    /**
     * 该文件夹下的文件
     */
    private List<HDFSFileInfoResponse> files;
    /**
     * 该文件夹下的其他文件夹
     */
    private List<HDFSFileDirResponse> dirs;
    private String permission;
    private String replication = "-";
    private String owner;
    private String group;
    private Integer size = 0;
    private String modificationTime;

    public HDFSFileDirResponse(HDFSFileDir fileDir) {
        this.relativePath = fileDir.getRelativePath();
        FsPermission dirPerimssion = fileDir.getPermission();
        this.permission = "d" +
                dirPerimssion.getUserAction().SYMBOL +
                dirPerimssion.getGroupAction().SYMBOL +
                dirPerimssion.getOtherAction().SYMBOL;
        this.owner = fileDir.getOwner();
        this.group = fileDir.getGroup();
        this.modificationTime = Time.formatTime(fileDir.getModification_time());
        this.dirs = fileDir.getDirs()
                .stream()
                // 将HDFSFileDir转为HDFSFileDirResponse
                .map(dir -> new HDFSFileDirResponse(dir))
                // 转回List结构
                .collect(Collectors.toList());
        this.files = fileDir.getFiles()
                .stream()
                // 将HDFSFileInfo转为HDFSFileInfoResponse
                .map(file -> new HDFSFileInfoResponse(file))
                // 转回List结构
                .collect(Collectors.toList());

    }
}
