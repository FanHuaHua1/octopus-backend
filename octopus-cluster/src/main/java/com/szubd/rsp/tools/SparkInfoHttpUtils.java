package com.szubd.rsp.tools;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @projectName: octopus-web
 * @package: com.szubd.rsp.tools
 * @className: SparkInfoHttpUtils
 * @author: Chandler
 * @description: 获取sparkInfo的Http调用方法封装
 * @date: 2023/8/24 上午 10:01
 * @version: 1.0
 */
@Slf4j
public class SparkInfoHttpUtils {
    public static String httpGET(String addr){
        StringBuilder data = new StringBuilder();
        try {
            URL url = new URL(addr);
            // 打开和url之间的连接
            HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();

            // 请求头
            urlConn.setRequestProperty("Accept-Charset", "utf-8");
            urlConn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
            urlConn.setDoOutput(true);
            urlConn.setDoInput(true);
            urlConn.setRequestMethod("GET");// GET和POST必须全大写
            urlConn.connect();

            // 获得响应码
            int code = urlConn.getResponseCode();
            // 响应成功，获得响应的数据
            if(code == 200) {
                // 得到数据流（输入流）
                InputStream is = urlConn.getInputStream();
                byte[] buffer = new byte[1024];
                int length = 0;
                while ((length = is.read(buffer)) != -1) {
                    String res = new String(buffer, 0, length);
                    data.append(res);
                }
            }
            // 断开连接
            urlConn.disconnect();

        } catch (Exception e) {
            log.warn("[SPARK-INFO] " + "获取Spark任务相关信息时出现网络连接错误");
        }
        return data.toString();
    }

    public static JSONObject httpGetRunningAppList4YarnServer(String resourceManager){
        // Yarn的ResourceManager获取的是JSONObject
        // 注意是JSONObject还是JSONObjectArray
        return (JSONObject) JSONObject.parse(httpGET("http://" + resourceManager + "/ws/v1/cluster/apps?state=RUNNING"));
    }

    public static JSONArray httpGetAppList4SparkServer(String historyServer){
        // SparkHistoryServer获取的是JSONArray
        return (JSONArray) JSONObject.parse(httpGET("http://" + historyServer + "/api/v1/applications/"));
    }

    public static JSONArray httpGetAppList4SparkServerLimit15(String historyServer){
        // SparkHistoryServer获取的是JSONArray
        return (JSONArray) JSONObject.parse(httpGET("http://" + historyServer + "/api/v1/applications/?limit=15"));
    }

    public static JSONObject httpGetAppData(String parentEndpoint){
        // 获取Application信息，返回JSONObject
        return (JSONObject) JSONObject.parse(httpGET(parentEndpoint));
    }

    // 获取JobData信息，返回JSONArray
    public static JSONArray httpGetJobData(String parentEndpoint){
        return (JSONArray) JSONObject.parse(httpGET(parentEndpoint + "/jobs"));
    }

    // 根据id获取StageData信息，返回JSONArray
    public static JSONObject httpGetStageData(String parentEndpoint, String stageId){
        return ((JSONArray) JSONObject.parse(httpGET(parentEndpoint + "/stages/" + stageId))).getJSONObject(0);
    }

    // 重载，stageId为空值时，返回的回是全量的数据
    public static JSONArray httpGetStageData(String parentEndpoint){
        return (JSONArray) JSONObject.parse(httpGET(parentEndpoint + "/stages"));
    }

    // 获取全量的ExecData信息，返回JSONArray
    public static JSONArray httpGetExecData(String parentEndpoint){
        return (JSONArray) JSONObject.parse(httpGET(parentEndpoint + "/allexecutors"));
    }

    // 获取EnvData信息，返回JSONObject
    public static JSONObject httpGetEnvData(String parentEndpoint){
        return (JSONObject) JSONObject.parse(httpGET(parentEndpoint + "/environment"));
    }
}
