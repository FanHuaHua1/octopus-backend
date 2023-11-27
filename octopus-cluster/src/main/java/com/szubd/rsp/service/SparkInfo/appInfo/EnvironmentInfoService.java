package com.szubd.rsp.service.SparkInfo.appInfo;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.szubd.rsp.sparkInfo.appInfo.EnvironmentInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import static com.szubd.rsp.tools.SparkInfoHttpUtils.httpGetEnvData;

/**
 * @projectName: octopus-web
 * @package: com.szubd.rsp.service.SparkInfo.appInfo
 * @className: EnvironmentInfoService
 * @author: Chandler
 * @description: EnvironmentInfo的方法：初始化
 * @date: 2023/8/24 下午 4:20
 * @version: 1.0
 */
@Slf4j
@Component
public class EnvironmentInfoService {
    // 初始化EnvironmentInfo
    public EnvironmentInfo initEnvironmentInfo(String parentEndpoint){
        EnvironmentInfo environmentInfo = new EnvironmentInfo();
        HashMap<String, String> sparkProperties = new HashMap<>();
        HashMap<String, String> systemProperties = new HashMap<>();
        HashMap<String, String> classpathEntries = new HashMap<>();
        ArrayList<String> jarsAddByUser = new ArrayList<>();
        // 获取信息
        JSONObject envJSONObject;
        try {
            envJSONObject = httpGetEnvData(parentEndpoint);
            environmentInfo.setRuntime(initRuntimeInfo(envJSONObject.getJSONObject("runtime")));
        } catch (Exception e) {
            log.error("[SPARK-INFO] " + "初始化EnvironmentInfo时无法从SparkHistoryServer中获取Environment信息, 生成空的EnvironmentInfo" + ", parentEndpoint: " +  parentEndpoint);
            environmentInfo.setRuntime(new EnvironmentInfo.RuntimeInfo());
            return environmentInfo;
        }
        // 封装信息
        for (Object obj: envJSONObject.getObject("sparkProperties", JSONArray.class)) {
            sparkProperties.put(((JSONArray)obj).getString(0),((JSONArray)obj).getString(1));
        }
        for (Object obj: envJSONObject.getObject("systemProperties", JSONArray.class)) {
            systemProperties.put(((JSONArray)obj).getString(0),((JSONArray)obj).getString(1));
        }
        for (Object obj: envJSONObject.getObject("classpathEntries", JSONArray.class)) {
            String k = ((JSONArray) obj).getString(1);
            if ("Added By User".equals(k)){
                jarsAddByUser.add(((JSONArray)obj).getString(0));
            }
            classpathEntries.put(((JSONArray)obj).getString(0),k);
        }
        // set方法更新值
        environmentInfo.setSparkProperties(sparkProperties);
        environmentInfo.setSystemProperties(systemProperties);
        environmentInfo.setClasspathEntries(classpathEntries);
        environmentInfo.setJarsAddByUser(jarsAddByUser);
        environmentInfo.setTimestamp(new Date(System.currentTimeMillis()));
        return environmentInfo;
    }

    // 初始化内部类RuntimeInfo
    private EnvironmentInfo.RuntimeInfo initRuntimeInfo(JSONObject o){
        EnvironmentInfo.RuntimeInfo runtimeInfo = new EnvironmentInfo.RuntimeInfo();
        runtimeInfo.setJavaVersion(o.getString("javaVersion"));
        runtimeInfo.setJavaHome(o.getString("javaHome"));
        runtimeInfo.setScalaVersion(o.getString("scalaVersion"));
        return runtimeInfo;
    }
}
