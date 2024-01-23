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
public class GlobalRSPInfo implements Serializable {
    private int id;
    private String superName;
    private String globalrspName;
    private int blocks;
    private long length;
    private long avgBlockSize;
    private int nodeId;
    private boolean isModified;
    private boolean isDeleted;
    private boolean isSync;

    public GlobalRSPInfo(String superName, String globalrspName, int nodeId) {
        this.superName = superName;
        this.globalrspName = globalrspName;
        this.blocks = -1;
        this.length = -1;
        this.avgBlockSize = -1;
        this.nodeId = nodeId;
        this.isModified = false;
        this.isDeleted = false;
        this.isSync = false;
    }
}
