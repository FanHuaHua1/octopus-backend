package com.szubd.rsp.integration;

/**
 * @Author Lingxiang Zhao
 * @Date 2023/10/11 14:32
 * @desc
 */
public interface IntegrationDubboService {

    void recordModels(int jobId, Object models);
    void recordModelPaths(int jobId, String Path);
    void checkAndIntegrate(int jobId, String algoType, String algoName, String args);
}
