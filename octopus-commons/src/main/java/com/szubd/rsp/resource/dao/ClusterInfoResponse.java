package com.szubd.rsp.resource.dao;


import com.szubd.rsp.resource.bean.ClusterInfo;
import lombok.Data;

import java.io.Serializable;

@Data
public class ClusterInfoResponse implements Serializable {
    private ClusterInfo clusterInfo;
}
