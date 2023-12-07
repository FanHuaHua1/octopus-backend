package com.szubd.rsp.algo;

import java.util.List;

public interface OperationDubboService {
    void toRsp(int jobId, String originName, String targetName, String blockNum, String type) throws Exception;

    void toRspMix(List<String>[] fileList, String father, int jobId, int rspMixParams, String mixType) throws Exception;
    void rspMerge(List<List<String>> fileList, String father, int jobId, int rspMixParams, String mixType) throws Exception;
    void distcpBeforeMix(List<String> fileList, String father, int jobId, int fatherJobId) throws Exception;
}
