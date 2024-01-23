package com.szubd.rsp.file;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class LocalRSPInfo implements Serializable {
    private int id;
    private String superName;
    private String name;
    private int blocks;
    private long length;
    private long avgBlockSize;
    private int nodeId;
    private boolean isModified;
    private boolean isDeleted;
    private boolean isSync;

    public LocalRSPInfo(String superName, String name, int nodeId) {
        this.superName = superName;
        this.name = name;
        this.blocks = -1;
        this.length = -1;
        this.avgBlockSize = -1;
        this.nodeId = nodeId;
        this.isModified = false;
        this.isDeleted = false;
        this.isSync = false;
    }
}
