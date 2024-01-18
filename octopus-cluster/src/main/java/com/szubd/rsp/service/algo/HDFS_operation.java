package com.szubd.rsp.service.algo;

import lombok.Data;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

@Data
public class HDFS_operation {
    private static final Logger logger = LoggerFactory.getLogger(HDFS_operation.class);
    private String hdfsUserName;
    @Value("${hdfs.nameNode.url}")
    private String nameNodeIP;


    private Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("dfs.support.append", "true");
        configuration.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        return configuration;
    }

    private FileSystem getFileSystem() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fs = null;
        fs = FileSystem.get(new URI(this.nameNodeIP), getConfiguration(), this.hdfsUserName);
        return fs;
    }

    public boolean mkdir(String path) {
        FileSystem fs = null;
        boolean isOk = false;
        if (StringUtils.isEmpty(path)) {
            return false;
        }
        try {
            // 路径已存在目录或文件
            if (existFile(path)) {
                logger.info("HDFS mkdir exist {}", path);
                return true;
            }
            // 目标路径
            fs = getFileSystem();
            Path srcPath = new Path(path);
            isOk = fs.mkdirs(srcPath);
            logger.info("HDFS mkdir success: {}", path);
        } catch (IOException | URISyntaxException | InterruptedException e) {
            logger.error("hdfs mkdir exception", e);

        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return isOk;
    }

    public boolean existFile(String path) {
        Boolean isExists = false;
        FileSystem fs = null;
        if (StringUtils.isEmpty(path)) {
            return true;
        }
        try {
            fs = getFileSystem();
            Path srcPath = new Path(path);
            isExists = fs.exists(srcPath);
        } catch (IOException | URISyntaxException | InterruptedException e) {

        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return isExists;
    }
    public boolean createFile(String path, String context) throws URISyntaxException, IOException, InterruptedException {
        //判断是否有该路径
        while(existFile(path)){
            String[] split = path.split("-");
            String[] strings = split[1].split("\\.");
            int i = Integer.parseInt(strings[0]);
            i++;
            path = split[0] + "-" + Integer.toString(i) + ".txt";
            logger.info("将要保存的文件名为：" + path);
        }
        Path srcPath = new Path(path);
        FileSystem fileSystem = getFileSystem();
        FSDataOutputStream outputStream = fileSystem.create(srcPath);
        byte[] bytes = context.getBytes();
        outputStream.write(bytes);
        outputStream.close();
        fileSystem.close();
        logger.info("模型保存成功");
        return true;
    }
}
