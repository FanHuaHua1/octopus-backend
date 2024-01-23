package com.szubd.rsp.sparkInfo.appInfo;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

/**
 * @projectName: octopus-web
 * @package: com.szubd.rsp.sparkInfo.appInfo
 * @className: EnvironmentInfo
 * @author: Chandler
 * @description: 自定义的类对象，用于记录环境信息
 * @date: 2023/8/23 下午 3:55
 * @version: 2.0
 */
@NoArgsConstructor
@Data
public class EnvironmentInfo implements Serializable {
    private static final long serialVersionUID = 10030L;
    public RuntimeInfo runtime;
    private HashMap<String,String> sparkProperties = new HashMap<>();
    private HashMap<String,String> systemProperties = new HashMap<>();
    private HashMap<String,String> classpathEntries = new HashMap<>();
    private ArrayList<String> jarsAddByUser = new ArrayList<>();
    private Date timestamp;

    @Data
    @NoArgsConstructor
    public static class RuntimeInfo implements Serializable{
        private static final long serialVersionUID = 10031L;
        public String javaVersion;
        public String javaHome;
        public String scalaVersion;
    }

    public int getSparkPropertiesCount() {
        return sparkProperties.size();
    }
    public int getSystemPropertiesCount() {
        return systemProperties.size();
    }
    public int getClasspathEntriesCount() {
        return classpathEntries.size();
    }

}
