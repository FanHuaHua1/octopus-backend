package com.szubd.rsp.service.resource.bean;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@Data
@ConfigurationProperties(prefix = "cluster")
public class Cluster {
    private String cdhApiPath;
    private String cdhApiPassword;
    private String rmHostIp;
    private String nnHostIp;
    private String nnHostBackupIp;
}