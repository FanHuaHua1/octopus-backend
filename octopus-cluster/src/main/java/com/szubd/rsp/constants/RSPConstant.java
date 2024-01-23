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
    @Value("${hdfs.prefix.origin}")
    public String originPrefix;

    @Value("${hdfs.prefix.localrsp}")
    public String localRspPrefix;

    @Value("${hdfs.prefix.globalrsp}")
    public String globalRspPrefix;

    @Value("${hdfs.prefix.tmp}")
    public String logPrefix;
    @Value("${hdfs.prefix.models}")
    public String modelPrefix;
    @Value("${hdfs.prefix.algo}")
    public String algoPrefix;



    @Value("${hdfs.url}")
    public String url;
    private FileSystem fileSystem;

    private FileSystem superFileSystem;

    public FileSystem getFileSystem() throws URISyntaxException, IOException, InterruptedException {
        if(fileSystem == null){
            URI uri = new URI(url);
            Configuration conf = new Configuration();
            String user = "zhaolingxiang";
            fileSystem = FileSystem.get(uri, conf, user);
        }
        return fileSystem;
    }

    public FileSystem getSuperFileSystem() throws URISyntaxException, IOException, InterruptedException {
        if(superFileSystem == null){
            URI uri = new URI(url);
            Configuration conf = new Configuration();
            String user = "hdfs";
            superFileSystem = FileSystem.get(uri, conf, user);
        }
        return superFileSystem;
    }

}
