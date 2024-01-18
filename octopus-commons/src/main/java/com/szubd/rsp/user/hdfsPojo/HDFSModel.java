package com.szubd.rsp.user.hdfsPojo;

import lombok.Data;

import java.io.Serializable;

@Data
public class HDFSModel implements Serializable {
    String name;
    String modelType;
    String modelSize;

    public HDFSModel(String name, String modelType, String modelSize) {
        this.name = name;
        this.modelType = modelType;
        this.modelSize = modelSize;
    }
}
