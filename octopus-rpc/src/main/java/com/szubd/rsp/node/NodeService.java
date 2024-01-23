package com.szubd.rsp.node;

import com.szubd.rsp.file.GlobalRSPInfo;
import com.szubd.rsp.file.LocalRSPInfo;
import com.szubd.rsp.file.OriginInfo;


import java.util.List;

public interface NodeService {
    List<OriginInfo> queryModifiedFile();

    List<LocalRSPInfo> queryModifiedLocalRspFile();

    List<GlobalRSPInfo> queryModifiedGlobalRspFile();

}
