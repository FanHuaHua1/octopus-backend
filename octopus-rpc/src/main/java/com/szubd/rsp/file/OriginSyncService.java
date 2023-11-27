package com.szubd.rsp.file;

import java.util.List;


public interface OriginSyncService {
    String sayHello(String name);

//    int createNewFile(String superName, String name);
//
//    int deleteFile(String superName, String name);
//
//    int updateByName(String superName, String name, long len, int blocks);
//
    //int updateModifiedStatus(List<RSPInfo> rspInfos);
    int updateInfos(List<OriginInfo> originInfos);
    int updateAndDeleteInfos(List<OriginInfo> updateOriginInfos, List<OriginInfo> deleteOriginInfos);
    int deleteInfos(List<OriginInfo> originInfos);

}
