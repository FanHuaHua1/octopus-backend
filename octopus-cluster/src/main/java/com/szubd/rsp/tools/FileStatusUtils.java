package com.szubd.rsp.tools;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class FileStatusUtils {

    public static void getFileStatus(String filePath) throws IOException, InterruptedException, URISyntaxException {
        URI uri = new URI("hdfs://172.31.238.105:8020");
        Configuration conf =  new Configuration();
        String user = "root";
        FileSystem fileSystem = FileSystem.get(uri, conf, user);
        FileStatus fileStatus = fileSystem.getFileStatus(new Path(filePath));
        long blockSize = fileStatus.getBlockSize();
        long len = fileStatus.getLen();
        BlockLocation[] fileBlockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, len);
        long blockNums = fileBlockLocations.length;
        fileSystem.close();
    }

    public static String getBlockStatus(String url, String filePath) throws IOException, InterruptedException, URISyntaxException {
        URI uri = new URI(url);
        Configuration conf =  new Configuration();
        String user = "root";
        FileSystem fileSystem = FileSystem.get(uri, conf, user);
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(filePath));
        //FileStatus fileStatus = fileSystem.getFileStatus();
        StringBuilder stringBuilder = new StringBuilder();
        for (FileStatus fileStatus : fileStatuses) {
            long len = fileStatus.getLen();
            BlockLocation[] fileBlockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, len);
            long blockNums = fileBlockLocations.length;
            stringBuilder.append(filePath);
            stringBuilder.append("\n");
//            logger.info("文件路径："+ filePath);
//            logger.info("文件大小："+ len);
//            logger.info("文件块数量："+ blockNums);
            for (BlockLocation fileBlockLocation : fileBlockLocations) {
                stringBuilder.append(fileBlockLocation);
            }
            stringBuilder.append("\n");
        }
        fileSystem.close();
        return stringBuilder.toString();
    }

    public static ImmutablePair<Integer, Long> calBlocksNumAndLength(FileSystem fileSystem, String filePath){
        FileStatus[] fileStatuses;
        try {
            fileStatuses = fileSystem.listStatus(new Path(filePath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        int blocks = 0;
        long len = 0L;
        for (FileStatus fileStatus : fileStatuses) {
            if(fileStatus.getLen() == 0 || fileStatus.isDirectory()){
                continue;
            }
            blocks++;
            len += fileStatus.getLen();
        }
        return ImmutablePair.of(blocks, len);
    }
}
