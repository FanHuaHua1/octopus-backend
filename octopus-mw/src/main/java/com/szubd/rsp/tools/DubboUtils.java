package com.szubd.rsp.tools;

import org.apache.dubbo.config.ReferenceConfig;


public class DubboUtils {

    /**
     * 使用示例：
     * OperationDubboService service = DubboUtils.getServiceRef(
     *                                    "172.31.238.102",
     *                                    "com.szubd.rsp.algo.OperationDubboService"
     *                                  );
     * @param ip
     * @param serviceName
     * @return
     * @param <T>
     */
    public static <T> T getServiceRef(String ip, String serviceName){
        ReferenceConfig<T> referenceConfig = new ReferenceConfig<>();
        referenceConfig.setInterface(serviceName);
        referenceConfig.setUrl("dubbo://"+ip+":20880/" + serviceName);
        return referenceConfig.get();
    }
}
