package com.szubd.rsp.file;
;

import com.szubd.rsp.file.GlobalRSPInfo;

import java.util.List;

public interface GlobalRSPSyncService {
    int updateInfos(List<GlobalRSPInfo> rspInfos);
    int updateAndDeleteInfos(List<GlobalRSPInfo> updateRspInfos, List<GlobalRSPInfo> deleteRspInfos);
    int deleteInfos(List<GlobalRSPInfo> rspInfos);

}
