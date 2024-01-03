package com.szubd.rsp.tools;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.szubd.rsp.service.algo.AlgoDubboServiceImpl;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class JobUtils {
    protected static final Logger logger = LoggerFactory.getLogger(JobUtils.class);
    protected static String getYarnLogUrl(String rmUrl, String applicationId) throws IOException {
        String rsUrl = rmUrl + "/ws/v1/cluster/apps/" + applicationId;
        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpGet httpGet = new HttpGet(rsUrl);
        CloseableHttpResponse resp = client.execute(httpGet);
        String logUrl = "Empty";
        if(HttpStatus.SC_OK == resp.getStatusLine().getStatusCode()) {
            HttpEntity entity = resp.getEntity();
            String appStr = IOUtils.toString(entity.getContent(), StandardCharsets.UTF_8);
            JSONObject jsonObject = JSON.parseObject(appStr);
            logUrl = jsonObject.getJSONObject("app").getString("amContainerLogs");
            return logUrl;
        }
        return logUrl;
    }

    public static String getYarnLog(String rmUrl, String applicationId) throws IOException {
        //TODO 暂时只是写成直接获取日志的方式，后续需要改成通过yarn的rest api获取
        String logUrl = getYarnLogUrl(rmUrl, applicationId);
        logger.info("fetched logurl: {}", logUrl);
        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpGet httpGet = new HttpGet(logUrl + "/stdout");
        CloseableHttpResponse resp = client.execute(httpGet);
        String log = "Empty";
        if(HttpStatus.SC_OK == resp.getStatusLine().getStatusCode()) {
            HttpEntity entity = resp.getEntity();
            String logStr = IOUtils.toString(entity.getContent(), StandardCharsets.UTF_8);
            Document document = Jsoup.parse(logStr);
            Elements elements = document.select("pre");
            for (Element element : elements) {
                log = element.text();
            }
        }
        client.close();
        return log;
    }
}
