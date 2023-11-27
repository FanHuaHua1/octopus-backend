package com.szubd.rsp.file;


import com.szubd.rsp.file.LocalRSPInfo;

import java.util.List;


public interface LocalRSPSyncService {
    int updateInfos(List<LocalRSPInfo> rspInfos);
    int updateAndDeleteInfos(List<LocalRSPInfo> updateRspInfos, List<LocalRSPInfo> deleteRspInfos);
    int deleteInfos(List<LocalRSPInfo> rspInfos);

}
