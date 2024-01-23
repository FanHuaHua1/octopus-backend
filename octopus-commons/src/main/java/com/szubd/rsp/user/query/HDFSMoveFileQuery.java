package com.szubd.rsp.user.query;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class HDFSMoveFileQuery implements Serializable {
    private List<String> filesPath;
    private String srcPath;

}
