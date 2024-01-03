package com.szubd.rsp.constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

@Component
public class RSPConstant {
    @Value("${dubbo.protocol.host}")
    public String ip;

    @Value("${hdfs.origin}")
    public String originPrefix;

    @Value("${hdfs.localrsp}")
    public String localRspPrefix;

    @Value("${hdfs.globalrsp}")
    public String globalRspPrefix;

    @Value("${hdfs.tmp}")
    public String logPrefix;

    @Value("${hdfs.models}")
    public String modelPrefix;

    @Value("${hdfs.algo}")
    public String algoPrefix;

    @Value("${hdfs.user}")
    public String user;

    @Value("${hdfs.url}")
    public String url;

    @Value("${hdfs.app}")
    public String app;
    private FileSystem fileSystem;

    private FileSystem superFileSystem;

    public FileSystem getFileSystem() throws URISyntaxException, IOException, InterruptedException {
        if(fileSystem == null){
            URI uri = new URI(url);
            Configuration conf = new Configuration();
            fileSystem = FileSystem.get(uri, conf, user);
        }
        return fileSystem;
    }

    public FileSystem getSuperFileSystem() throws URISyntaxException, IOException, InterruptedException {
        if(superFileSystem == null){
            URI uri = new URI(url);
            Configuration conf = new Configuration();
            superFileSystem = FileSystem.get(uri, conf, "hdfs");
        }
        return superFileSystem;
    }

}
