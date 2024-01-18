package com.szubd.rsp.user.hdfsPojo;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class HDFSModelType implements Serializable {
    private String name;
    private Integer nums;

//    public HDFSModelType(HDFSFileDir fileDir) {
//        List<HDFSFileInfo> files = fileDir.getFiles();
//        List<HDFSFileDir> dirs = fileDir.getDirs();
//        //files的长度理应跟dirs长度一致
//        for(int i = 0; i < files.size(); i++) {
//
//        }
//    }
    public HDFSModelType(String name, Integer nums) {
        this.name = name;
        this.nums = nums;
    }

}
