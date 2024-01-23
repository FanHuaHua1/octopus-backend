package com.szubd.rsp.hdfs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class HadoopUtils {
    public static String getNameNodeAdress() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/hdfs-site.xml"));
        conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/yarn-site.xml"));
        conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/core-site.xml"));
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        String host;
        String namenodeNameServiceId = DFSUtil.getNamenodeNameServiceId(conf);
        if(!HAUtil.isHAEnabled(conf, namenodeNameServiceId)){
            host = new URI(conf.get("fs.defaultFS")).getHost();
        } else {
            FileSystem system = FileSystem.get(new URI("hdfs://" + namenodeNameServiceId), conf,"zhaolingxiang");
            host = HAUtil.getAddressOfActive(system).getHostString();
            system.close();
        }
        return InetAddress.getByName(host).getHostAddress();
    }

    public static List<Map<String, Object>> getHDFSBlockAddress(String url, String filePath) throws IOException, InterruptedException, URISyntaxException {
        URI uri = new URI(url);
        Configuration conf =  new Configuration();
        String user = "zhaolingxiang";
        FileSystem fileSystem = FileSystem.get(uri, conf, user);
        BlockLocation[] fileBlockLocations = fileSystem.getFileBlockLocations(new Path(filePath), 0, Long.MAX_VALUE);
        List<Map<String, Object>> list = new LinkedList<>();
        for (BlockLocation fileBlockLocation : fileBlockLocations) {
            Map<String, Object> map = new HashMap<>();
            map.put("start", fileBlockLocation.getOffset());
            map.put("offset", fileBlockLocation.getLength());
            map.put("topologyPaths", Arrays.stream(fileBlockLocation.getTopologyPaths()).reduce((a, b) -> a + "," + b));
            map.put("storageIds", Arrays.stream(fileBlockLocation.getStorageIds()).reduce((a, b) -> a + "," + b));
            list.add(map);
        }
        fileSystem.close();
        return list;
    }
    public static String getBlockStatus(String url, String filePath) throws IOException, InterruptedException, URISyntaxException {
        URI uri = new URI(url);
        Configuration conf =  new Configuration();
        String user = "zhaolingxiang";
        FileSystem fileSystem = FileSystem.get(uri, conf, user);
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(filePath));
        StringBuilder stringBuilder = new StringBuilder();
        for (FileStatus fileStatus : fileStatuses) {
            long len = fileStatus.getLen();
            BlockLocation[] fileBlockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, len);
            long blockNums = fileBlockLocations.length;
            stringBuilder.append(fileStatus.getPath());
            stringBuilder.append("\n");
            System.out.println("文件路径："+ filePath);
            System.out.println("文件大小："+ len);
            System.out.println("文件块数量："+ blockNums);
            for (BlockLocation fileBlockLocation : fileBlockLocations) {
                stringBuilder.append(fileBlockLocation);
            }
            stringBuilder.append("\n");
        }
        fileSystem.close();
        return stringBuilder.toString();
    }

    public static List<Map<String, Object>> getBlockList(String url, String filePath, String superName, String name, Integer nodeId) throws IOException, InterruptedException, URISyntaxException {
        URI uri = new URI(url);
        Configuration conf =  new Configuration();
        String user = "zhaolingxiang";
        FileSystem fileSystem = FileSystem.get(uri, conf, user);
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(filePath));
        System.out.println(fileStatuses);
        List<Map<String, Object>> list = new LinkedList<>();
        for (FileStatus fileStatus : fileStatuses) {
            Map<String, Object> map = new HashMap<>();
            map.put("superName", superName);
            map.put("name", name);
            map.put("nodeId", nodeId);
            map.put("blockName", fileStatus.getPath().getName());
            map.put("length", fileStatus.getLen());
            map.put("modificationTime", fileStatus.getModificationTime());
            BlockLocation[] fileBlockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
            map.put("hdfsBlockNum", fileBlockLocations.length);
            list.add(map);
        }
        fileSystem.close();
        return list;
    }

    public static FileStatus getFileStatus(String url, String path) throws IOException, InterruptedException, URISyntaxException {
        System.out.println(url);
        System.out.println(path);
        URI uri = new URI(url);
        Configuration conf = new Configuration();
        String user = "zhaolingxiang";
        FileSystem fileSystem = FileSystem.get(uri, conf, user);
        FileStatus fileStatus = fileSystem.getFileStatus(new Path(path));
        fileSystem.close();
        return fileStatus;
    }
}
