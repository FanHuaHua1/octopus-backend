package com.szubd.rsp.user.query;

import lombok.Data;

import java.util.List;

@Data
public class HDFSDeleteFilesQuery {
    private List<String> paths = null;
}
