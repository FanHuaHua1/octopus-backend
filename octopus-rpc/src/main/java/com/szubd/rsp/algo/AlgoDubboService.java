package com.szubd.rsp.algo;

public interface AlgoDubboService {
    void toAlgo(
            int jobId,
            String algoType,
            String algoSubSetting,
            String algoName,
            String filename,
            int executorNum,
            int executorMemory,
            int executorCores,
            String args) throws Exception;
    void toAlgoLogo(
            String userId,
            String algoType,
            String algoSubSetting,
            String algoName,
            String filename,
            int executorNum,
            int executorMemory,
            int executorCores,
            String modelType,
            String model,
            String... args) throws Exception;
}
