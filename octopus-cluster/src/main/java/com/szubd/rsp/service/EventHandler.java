package com.szubd.rsp.service;

import com.szubd.rsp.file.OriginInfo;
import com.szubd.rsp.file.OriginSyncService;
import com.szubd.rsp.constants.RSPConstant;
import com.szubd.rsp.dao.GlobalRspDao;
import com.szubd.rsp.dao.LocalRspDao;
import com.szubd.rsp.dao.OriginDao;
import com.szubd.rsp.file.GlobalRSPInfo;
import com.szubd.rsp.file.LocalRSPInfo;
import com.szubd.rsp.service.init.NodeInfoQueryService;
import org.apache.dubbo.config.annotation.DubboReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.inotify.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Component
public class EventHandler {

    @Autowired
    private OriginDao originDao;
    @Autowired
    private LocalRspDao localRspDao;
    @Autowired
    private GlobalRspDao globalRspDao;
    @Autowired
    private RSPConstant constant;
    private static final HashMap<String, Boolean> existMap;
    private static final HashMap<String, Boolean> existLocalRspMap;
    private static final HashMap<String, Boolean> existGlobalRspMap;

    protected static final Logger logger = LoggerFactory.getLogger(EventHandler.class);

    static {
        existMap = new HashMap<>();
        existLocalRspMap = new HashMap<>();
        existGlobalRspMap = new HashMap<>();
    }

    public static HashMap<String, Boolean> getMap(){
        return existMap;
    }

    public static HashMap<String, Boolean> getLocalRspMap(){
        return existLocalRspMap;
    }

    public static HashMap<String, Boolean> existGlobalRspMap(){
        return existGlobalRspMap;
    }


    //1. 上传一个文件夹，会先CREATE一个文件夹，然后对于每个文件夹里的文件
    //   会先对该文件的_COPYING_后缀文件进行CREATE，CLOSE，接着RENAME，
    //2. 仅对子级目录进行管理
    public void handleCreateEvent(Event.CreateEvent event){
        String path = event.getPath();
        if(path.startsWith(constant.originPrefix)){
            handleOriginCreateEvent(path);
        } else if(path.startsWith(constant.localRspPrefix)){
            handleLocalRspCreateEvent(path);
        } else if(path.startsWith(constant.globalRspPrefix)){
            handleGlobalRspCreateEvent(path);
        }
    }

    public void handleOriginCreateEvent(String path){
        String[] split = path.replaceFirst(constant.originPrefix, "").split("/");
        //对于父级目录，不用管，例如/user/zhaolingxiang/rsp/xxx
        if(split.length == 1){
            return;
        }
        //对于子级目录：
        //  1.更新检测的map，
        //  2.在数据库中创建
        //对于子级目录下：
        //  1.仅仅更新map，提醒系统扫描更新数据库内容
        existMap.put(split[0] + "/" + split[1], true);
        if(split.length >= 3){
            return;
        }
        originDao.createNewFile(split[0], split[1]);
    }

    public void handleLocalRspCreateEvent(String path){
        String[] split = path.replaceFirst(constant.localRspPrefix, "").split("/");
        if(split.length == 1){
            return;
        }
        existLocalRspMap.put(split[0] + "/" + split[1], true);
        if(split.length >= 3){
            return;
        }
        localRspDao.createNewFile(split[0], split[1]);
    }

    public void handleGlobalRspCreateEvent(String path){
        String[] split = path.replaceFirst(constant.globalRspPrefix, "").split("/");
        //对于父级目录，不用管，例如/user/zhaolingxiang/rsp/xxx
        if(split.length == 1){
            return;
        }
        //对于子级目录：
        //  1.更新检测的map，
        //  2.在数据库中创建
        //对于子级目录下：
        //  1.仅仅更新map，提醒系统扫描更新数据库内容
        existGlobalRspMap.put(split[0] + "/" + split[1], true);
        if(split.length >= 3){
            return;
        }
        globalRspDao.createNewFile(split[0], split[1]);
    }

    public void handleRenameEvent(Event.RenameEvent event) throws Exception {
        String origin = event.getSrcPath();
        String dst = event.getDstPath();
        //如果原路径和终路径都不包含 rspPrefix， 忽略该事件。
        if((!origin.startsWith(constant.originPrefix) && !dst.startsWith(constant.originPrefix)
                && !origin.startsWith(constant.localRspPrefix) && !dst.startsWith(constant.localRspPrefix)
                && !origin.startsWith(constant.globalRspPrefix) && !dst.startsWith(constant.globalRspPrefix))
                || (origin.startsWith(constant.originPrefix) && dst.startsWith(constant.localRspPrefix))
                || (origin.startsWith(constant.localRspPrefix) && dst.startsWith(constant.originPrefix))
                || (origin.startsWith(constant.localRspPrefix) && dst.startsWith(constant.globalRspPrefix))
                || (origin.startsWith(constant.globalRspPrefix) && dst.startsWith(constant.originPrefix))
                || (origin.startsWith(constant.globalRspPrefix) && dst.startsWith(constant.localRspPrefix))
                || (origin.startsWith(constant.originPrefix) && dst.startsWith(constant.globalRspPrefix))
            ){
            return;
        }
        //移出事件
        if(origin.startsWith(constant.originPrefix) || origin.startsWith(constant.localRspPrefix)|| origin.startsWith(constant.globalRspPrefix)){
            if(origin.startsWith(constant.originPrefix)){
                originMoveOut(origin, dst);
            } else if(origin.startsWith(constant.localRspPrefix)){
                localRspMoveOut(origin, dst);
            } else {
                globalRspMoveOut(origin, dst);
            }
        } else {
            if(dst.startsWith(constant.originPrefix)){
                originMoveIn(dst);
            } else if(dst.startsWith(constant.localRspPrefix)){
                localRspMoveIn(dst);
            } else {
                globalRspMoveIn(dst);
            }
        }
    }

    /**
     *  可能事件：
     *  1.  原路径和终路径都包含 rspPrefix【文件夹内移动】
     *  2.  仅原路径 rspPrefix：【移走了】
     *  /user/zhaolingxiang/rsp/testmulti66
     * @param origin
     * @param dst
     */
    public void originMoveOut(String origin, String dst){
        if(dst.startsWith(constant.originPrefix)){
            //源包含，终包含，可能事件
            //后缀文件夹内的互相移动
            //上传文件时候的rename事件
            String[] split = origin.replaceFirst(constant.originPrefix, "").split("/");
            int l = split.length;
            if(l > 2){
                //大于2说明是son文件夹里面的文件行为，可以忽略
                existMap.put(split[0] + "/" + split[1], true);
                return;
            }
            logger.info("原始文件-改名/移动事件");
            deleteAndCreateNewFile(origin, dst);
        } else {
            //源包含，终不包含，可能事件
            //从后缀文件夹移除或删除
            logger.info("原始文件-删除/移出文件夹事件");
            deletePath(origin);
        }
    }

    /**
     *  可能事件：
     *  3.  仅终路径 rspPrefix：【来源于其他文件夹】
     *  /user/zhaolingxiang/rsp/testmulti66
     * @param dst
     * @throws Exception
     */
    public void originMoveIn(String dst) throws Exception {
        createRenameFile(dst);
    }

    public void globalRspMoveOut(String origin, String dst){
        //可能事件：
        //3.  仅终路径 rspPrefix：【来源于其他文件夹】
        // /user/zhaolingxiang/rsp/testmulti66
        if(dst.startsWith(constant.globalRspPrefix)){
            //源包含，终包含，可能事件
            //后缀文件夹内的互相移动
            //上传文件时候的rename事件
            String[] split = origin.replaceFirst(constant.globalRspPrefix, "").split("/");
            int l = split.length;
            if(l > 2){
                //大于2说明是son文件夹里面的文件行为，可以忽略
                existGlobalRspMap.put(split[0] + "/" + split[1], true);
                return;
            }
            logger.info("全局RSP文件-改名/移动事件");
            deleteAndCreateNewGlobalRspFile(origin, dst);
        } else {
            //源包含，终不包含，可能事件
            //从后缀文件夹移除或删除
            logger.info("全局RSP文件-删除/移出文件夹事件");
            deleteGlobalRspPath(origin);
        }
    }
    public void localRspMoveOut(String origin, String dst){
        //可能事件：
        //3.  仅终路径 rspPrefix：【来源于其他文件夹】
        // /user/zhaolingxiang/rsp/testmulti66
        if(dst.startsWith(constant.localRspPrefix)){
            //源包含，终包含，可能事件
            //后缀文件夹内的互相移动
            //上传文件时候的rename事件
            String[] split = origin.replaceFirst(constant.localRspPrefix, "").split("/");
            int l = split.length;
            if(l > 2){
                //大于3说明是grandson文件夹里面的文件行为，可以忽略
                existLocalRspMap.put(split[0] + "/" + split[1], true);
                return;
            }
            logger.info("本地RSP文件-改名/移动事件");
            deleteAndCreateNewLocalRspFile(origin, dst);
        } else {
            //源包含，终不包含，可能事件
            //从后缀文件夹移除或删除
            logger.info("本地RSP文件-删除/移出文件夹事件");
            deleteLocalRspPath(origin);
        }
    }
    public void localRspMoveIn(String dst) throws Exception {
        createLocalRspRenameFile(dst);
    }

    public void globalRspMoveIn(String dst) throws Exception {
        createGlobalRspRenameFile(dst);
    }


    public void deletePath(String path){
        logger.info("原始文件-删除事件");
        String[] split = path.replaceFirst(constant.originPrefix, "").split("/");
        String superName = split[0];
        if(split.length == 1){
            //如果删除/移出了整个文件夹
            originDao.deleteFatherDirectoryTemp(superName);
        } else {
            //如果删除/移出了子文件夹
            String name = split[1];
            originDao.deleteSonDirectoryTemp(superName, name);
        }
    }
    public void deleteGlobalRspPath(String path){
        logger.info("全局RSP文件-删除事件");
        String[] split = path.replaceFirst(constant.globalRspPrefix, "").split("/");
        String superName = split[0];
        if(split.length == 1){
            //如果删除/移出了整个文件夹
            globalRspDao.deleteFatherDirectoryTemp(superName);
        } else {
            //如果删除/移出了子文件夹
            String name = split[1];
            globalRspDao.deleteSonDirectoryTemp(superName, name);
        }
    }

    public void deleteLocalRspPath(String path){
        logger.info("本地RSP文件-删除事件");
        String[] split = path.replaceFirst(constant.localRspPrefix, "").split("/");
        String superName = split[0];
        if(split.length == 1){
            //如果删除/移出了整个文件夹
            localRspDao.deleteFatherDirectoryTemp(superName);
        } else if(split.length == 2){
            //如果删除/移出了子文件夹
            String name = split[1];
            localRspDao.deleteSonDirectoryTemp(superName, name);
        }
    }

    public void deleteAndCreateNewFile(String originPath, String dstPath){
        String[] oriSplit = originPath.replaceFirst(constant.originPrefix, "").split("/");
        String[] dstSplit = dstPath.replaceFirst(constant.originPrefix, "").split("/");
        //限制只能同级目录下的移动
        if(oriSplit.length != dstSplit.length){
            return;
        }
        if(oriSplit.length == 1){
            //整个父级移动了
            List<OriginInfo> originInfos = originDao.queryBySuperName(oriSplit[0]);
            for (OriginInfo originInfo : originInfos) {
                originInfo.setSuperName(dstSplit[0]);
            }
            originDao.deleteFatherDirectoryTemp(oriSplit[0]);
            originDao.insertFiles(originInfos);
        } else if(oriSplit.length == 2){
            //子级移动
            OriginInfo originInfo = originDao.queryBySuperNameAndName(oriSplit[0], oriSplit[1]);
            originInfo.setSuperName(dstSplit[0]);
            originInfo.setName(dstSplit[1]);
            originDao.deleteSonDirectoryTemp(oriSplit[0], oriSplit[1]);
            originDao.insertFile(originInfo);
        }
    }

    public void deleteAndCreateNewGlobalRspFile(String originPath, String dstPath){
        String[] oriSplit = originPath.replaceFirst(constant.globalRspPrefix, "").split("/");
        String[] dstSplit = dstPath.replaceFirst(constant.globalRspPrefix, "").split("/");
        //限制只能同级目录下的移动
        if(oriSplit.length != dstSplit.length){
            return;
        }
        if(oriSplit.length == 1){
            //整个父级移动了
            List<GlobalRSPInfo> globalRspDaos = globalRspDao.queryBySuperName(oriSplit[0]);
            for (GlobalRSPInfo rspInfo : globalRspDaos) {
                rspInfo.setSuperName(dstSplit[0]);
            }
            globalRspDao.deleteFatherDirectoryTemp(oriSplit[0]);
            globalRspDao.insertFiles(globalRspDaos);
        } else if(oriSplit.length == 2){
            //子级移动
            GlobalRSPInfo globalRSPInfo = globalRspDao.queryBySuperNameAndGlobalrspName(oriSplit[0], oriSplit[1]);
            globalRSPInfo.setSuperName(dstSplit[0]);
            globalRSPInfo.setGlobalrspName(dstSplit[1]);
            globalRspDao.deleteSonDirectoryTemp(oriSplit[0], oriSplit[1]);
            globalRspDao.insertFile(globalRSPInfo);
        }
    }

    public void deleteAndCreateNewLocalRspFile(String originPath, String dstPath){
        String[] oriSplit = originPath.replaceFirst(constant.localRspPrefix, "").split("/");
        String[] dstSplit = dstPath.replaceFirst(constant.localRspPrefix, "").split("/");
        //限制只能同级目录下的移动
        if(oriSplit.length != dstSplit.length){
            return;
        }
        if(oriSplit.length == 1){
            //整个父级移动了
            List<LocalRSPInfo> localRSPInfos = localRspDao.queryBySuperName(oriSplit[0]);
            for (LocalRSPInfo localRSPInfo : localRSPInfos) {
                localRSPInfo.setSuperName(dstSplit[0]);
            }
            localRspDao.deleteFatherDirectoryTemp(oriSplit[0]);
            localRspDao.insertFiles(localRSPInfos);
        } else if(oriSplit.length == 2){
            //整个子级移动了
            LocalRSPInfo localRSPInfo = localRspDao.queryBySuperNameAndName(oriSplit[0], oriSplit[1]);
            localRSPInfo.setSuperName(dstSplit[0]);
            localRSPInfo.setName(dstSplit[1]);
            localRspDao.deleteSonDirectoryTemp(oriSplit[0],oriSplit[1]);
            localRspDao.insertFile(localRSPInfo);
        }
    }


    /**
     * 其他文件夹移入
     * @param path
     */
    public void createRenameFile(String path) throws Exception {
        String[] split = path.replaceFirst(constant.originPrefix, "").split("/");
        if(split.length > 2){
            return;
        }
//        String user = "root";
//        URI url = new URI(path);
//        Configuration conf = new Configuration();
//        FileSystem fileSystem = FileSystem.get(url, conf, user);
        FileSystem fileSystem = constant.getFileSystem();

        //移动到父级文件夹（得默认移动进来的文件夹是父级文件夹）
        if(split.length == 1){
            logger.info("原始文件-移动-深度为1");
            String superName = split[0];
            //fileStatuses代表多个子级目录
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(path));
            List<OriginInfo> originInfos = new ArrayList<>();
            for (FileStatus fileStatus : fileStatuses) {
                if(!fileStatus.isDirectory()){
                    logger.warn("origin：有不符合条件的createRenameFile事件" + fileStatus.getPath());
                    continue;
                }
                int blocks = 0;
                long len = 0L;
                String name = fileStatus.getPath().getName();
                Path sonPath = fileStatus.getPath();
                //System.out.println("sonPath: " + sonPath);
                FileStatus[] blockStatuses = fileSystem.listStatus(sonPath);
                for (FileStatus blockStatus : blockStatuses) {
                    if(blockStatus.getLen() == 0){
                        continue;
                    }
                    blocks++;
                    len += blockStatus.getLen();
                }
                originInfos.add(new OriginInfo(0, superName, name, blocks, len, len / blocks, NodeInfoQueryService.nodeID , 0, 0,  true, true, true));
            }
            originDao.insertFiles(originInfos);
            //移动到子级文件夹（得默认移动进来的文件夹是子级文件夹）
        } else if(split.length == 2){
            logger.info("原始文件-移动-深度为2");
            String superName = split[0];
            String name = split[1];
            int blocks = 0;
            long len = 0L;
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(path));
            for (FileStatus fileStatus : fileStatuses) {
                if(fileStatus.getLen() == 0){
                    continue;
                }
                blocks++;
                len += fileStatus.getLen();
                originDao.insertFile(new OriginInfo(0, superName, name, blocks, len, len / blocks, NodeInfoQueryService.nodeID , 0, 0, true, true, true));
            }
        }
    }

    public void createLocalRspRenameFile(String path) throws Exception {
        String[] split = path.replaceFirst(constant.localRspPrefix, "").split("/");
        if(split.length > 2){
            return;
        }
//        String user = "root";
//        URI url = new URI(path);
//        Configuration conf = new Configuration();
//        FileSystem fileSystem = FileSystem.get(url, conf, user);
        FileSystem fileSystem = constant.getFileSystem();
        //移动到父级文件夹（得默认移动进来的文件夹是父级文件夹）
        if(split.length == 1){
            logger.info("本地RSP文件-移动-深度为1");
            String superName = split[0];
            //fileStatuses代表多个子级目录
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(path));
            List<LocalRSPInfo> localRspInfos = new ArrayList<>();
            for (FileStatus fileStatus : fileStatuses) {
                if(!fileStatus.isDirectory()){
                    logger.warn("origin：有不符合条件的createRenameFile事件" + fileStatus.getPath());
                    continue;
                }
                int blocks = 0;
                long len = 0L;
                String name = fileStatus.getPath().getName();
                Path sonPath = fileStatus.getPath();
                //System.out.println("sonPath: " + sonPath);
                FileStatus[] blockStatuses = fileSystem.listStatus(sonPath);
                for (FileStatus blockStatus : blockStatuses) {
                    if(blockStatus.getLen() == 0){
                        continue;
                    }
                    blocks++;
                    len += blockStatus.getLen();
                }
                localRspInfos.add(new LocalRSPInfo(0, superName, name, blocks, len, len / blocks, NodeInfoQueryService.nodeID, true, true, true));
            }
            localRspDao.insertFiles(localRspInfos);
            //移动到grandson级文件夹（得默认移动进来的文件夹是子级文件夹）
        } else if(split.length == 2){
            logger.info("本地RSP文件-移动-深度为2");
            String superName = split[0];
            String name = split[1];
            int blocks = 0;
            long len = 0L;
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(path));
            for (FileStatus fileStatus : fileStatuses) {
                if(fileStatus.getLen() == 0){
                    continue;
                }
                blocks++;
                len += fileStatus.getLen();
                localRspDao.insertFile(new LocalRSPInfo(0, superName, name, blocks, len, len / blocks, NodeInfoQueryService.nodeID , true, true, true));
            }
        }
    }
    public void createGlobalRspRenameFile(String path) throws Exception {
        String[] split = path.replaceFirst(constant.globalRspPrefix, "").split("/");
        if(split.length > 2){
            return;
        }
//        String user = "root";
//        URI url = new URI(path);
//        Configuration conf = new Configuration();
//        FileSystem fileSystem = FileSystem.get(url, conf, user);
        FileSystem fileSystem = constant.getFileSystem();
        //移动到父级文件夹（得默认移动进来的文件夹是父级文件夹）
        if(split.length == 1){
            logger.info("全局RSP文件-移动-深度为1");
            String superName = split[0];
            //fileStatuses代表多个子级目录
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(path));
            List<GlobalRSPInfo> rspInfos = new ArrayList<>();
            for (FileStatus fileStatus : fileStatuses) {
                if(!fileStatus.isDirectory()){
                    logger.warn("global：有不符合条件的createRenameFile事件" + fileStatus.getPath());
                    continue;
                }
                int blocks = 0;
                long len = 0L;
                String name = fileStatus.getPath().getName();
                Path sonPath = fileStatus.getPath();
                //System.out.println("sonPath: " + sonPath);
                FileStatus[] blockStatuses = fileSystem.listStatus(sonPath);
                for (FileStatus blockStatus : blockStatuses) {
                    if(blockStatus.getLen() == 0){
                        continue;
                    }
                    blocks++;
                    len += blockStatus.getLen();
                }
                rspInfos.add(new GlobalRSPInfo(0, superName, name, blocks, len, len / blocks, NodeInfoQueryService.nodeID ,  true, true, true));
            }
            globalRspDao.insertFiles(rspInfos);
            //移动到子级文件夹（得默认移动进来的文件夹是子级文件夹）
        } else if(split.length == 2){
            logger.info("全局RSP文件-移动-深度为2");
            String superName = split[0];
            String name = split[1];
            int blocks = 0;
            long len = 0L;
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(path));
            for (FileStatus fileStatus : fileStatuses) {
                if(fileStatus.getLen() == 0){
                    continue;
                }
                blocks++;
                len += fileStatus.getLen();
                globalRspDao.insertFile(new GlobalRSPInfo(0, superName, name, blocks, len, len / blocks, NodeInfoQueryService.nodeID , true, true, true));
            }
        }
    }
    public void updateByName(String superName, String name, long len, int blocks) {
        if(len == 0) return;
        originDao.updateByName(superName, name, len, blocks);
    }

    public void updateLocalrspByName(String superName, String name, long len, int blocks) {
        if(len == 0) return;
        localRspDao.updateByName(superName, name, len, blocks);
    }

    public void updateGlobalrspByName(String superName, String globalrspName, long len, int blocks) {
        if(len == 0) return;
        globalRspDao.updateByName(superName, globalrspName, len, blocks);
    }

//    public List<RSPInfo> queryForModifiedFile() {
//        return userDao.queryForModifiedFile();
//    }
//
//    public List<RSPInfo> queryBySuperName(String superName) {
//        return userDao.queryBySuperName(superName);
//    }
//
//    public RSPInfo queryBySuperNameAndName(String superName, String name) {
//        return userDao.queryBySuperNameAndName(superName, name);
//    }
//    public void updateModifiedStatus(List<RSPInfo> rspInfos) {
//        userDao.updateModifiedStatus(rspInfos);
//    }

}
