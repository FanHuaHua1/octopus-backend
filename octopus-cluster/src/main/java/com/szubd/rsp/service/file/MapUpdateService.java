package com.szubd.rsp.service.file;

import com.szubd.rsp.constants.RSPConstant;
import com.szubd.rsp.service.EventHandler;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.szubd.rsp.tools.FileStatusUtils.calBlocksNumAndLength;


@Component
public class MapUpdateService {

    @Autowired
    protected RSPConstant rspConstant;
    @Autowired
    protected EventHandler handler;
    protected FileSystem fileSystem;

    protected static final Logger logger = LoggerFactory.getLogger(MapUpdateService.class);

    @PostConstruct
    public void initFileSystem() throws URISyntaxException, IOException, InterruptedException {
        fileSystem = rspConstant.getFileSystem();
    }

    @Scheduled(fixedRate = 5000)
    @Async("taskExecutor")
    public void runScheduleOrigin() {
        ConcurrentHashMap<String, Boolean> map = EventHandler.getMap();
        if(!map.isEmpty()){
            logger.info("Origin需要更新:{}", map);
        }
        for(Iterator<Map.Entry<String, Boolean>> iterator = map.entrySet().iterator();iterator.hasNext();){
            Map.Entry<String, Boolean> next = iterator.next();
            if(!next.getValue()){
                iterator.remove();
            } else {
                next.setValue(false);
                String[] split = next.getKey().split("/");
                ImmutablePair<Integer, Long> calRes = calBlocksNumAndLength(fileSystem, rspConstant.originPrefix + next.getKey());
                logger.info("文件大小：{}", calRes.getRight());
                logger.info("文件块数量：{}", calRes.getLeft());
                handler.updateByName(split[0], split[1], calRes.getRight(), calRes.getLeft());
            }
        }
    }

    @Scheduled(fixedRate = 5000)
    @Async("taskExecutor")
    public void runScheduleLocalRSP() {
        ConcurrentHashMap<String, Boolean> map = EventHandler.getLocalRspMap();
        if(!map.isEmpty()){
            logger.info("LocalRSP需要更新:{}", map);
        }
        for(Iterator<Map.Entry<String, Boolean>> iterator = map.entrySet().iterator();iterator.hasNext();){
            Map.Entry<String, Boolean> next = iterator.next();
            if(!next.getValue()){
                iterator.remove();
            } else {
                next.setValue(false);
                String[] split = next.getKey().split("/");
                ImmutablePair<Integer, Long> calRes = calBlocksNumAndLength(fileSystem, rspConstant.localRspPrefix + next.getKey());
                logger.info("文件大小：{}", calRes.getRight());
                logger.info("文件块数量：{}", calRes.getLeft());
                handler.updateLocalrspByName(split[0], split[1], calRes.getRight(), calRes.getLeft());
            }
        }
    }

    @Scheduled(fixedRate = 5000)
    @Async("taskExecutor")
    public void runScheduleGlobalRSP() {
        ConcurrentHashMap<String, Boolean> map = EventHandler.existGlobalRspMap();
        if(!map.isEmpty()){
            logger.info("GlobalRSP需要更新:{}", map);
        }
        for(Iterator<Map.Entry<String, Boolean>> iterator = map.entrySet().iterator();iterator.hasNext();){
            Map.Entry<String, Boolean> next = iterator.next();
            if(!next.getValue()){
                iterator.remove();
            } else {
                next.setValue(false);
                String[] split = next.getKey().split("/");
                ImmutablePair<Integer, Long> calRes = calBlocksNumAndLength(fileSystem, rspConstant.globalRspPrefix + next.getKey());
                logger.info("文件大小：{}", calRes.getRight());
                logger.info("文件块数量：{}", calRes.getLeft());
                handler.updateGlobalrspByName(split[0], split[1], calRes.getRight(), calRes.getLeft());
            }
        }
    }

}
