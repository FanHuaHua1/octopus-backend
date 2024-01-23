package com.szubd.rsp.service.job;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class JobUtils {
    public static String getYarnLogUrl(String rmUrl, String applicationId) throws IOException {
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

    public static String getYarnLog(String logUrl) throws IOException {
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
