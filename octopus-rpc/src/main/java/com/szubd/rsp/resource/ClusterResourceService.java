package com.szubd.rsp.resource;




import com.szubd.rsp.resource.bean.ClusterInfo;
import com.szubd.rsp.resource.bean.ClusterMetrics;
import com.szubd.rsp.resource.bean.ClusterNode;

import java.util.List;


public interface ClusterResourceService {
  ClusterMetrics getClusterMetricsInfo();
  ClusterInfo getClusterInfo();
  List<ClusterNode> getClusterNodesInfo();
}
