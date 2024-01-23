package com.szubd.rsp.resource.dao;


import com.szubd.rsp.resource.bean.ClusterMetrics;
import lombok.Data;

import java.io.Serializable;

@Data
public class ClusterMetricsResponse implements Serializable {
    private ClusterMetrics clusterMetrics;
}
