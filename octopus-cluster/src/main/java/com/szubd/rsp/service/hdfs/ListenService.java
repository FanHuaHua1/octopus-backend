package com.szubd.rsp.service.hdfs;

import com.szubd.rsp.constants.RSPConstant;
import com.szubd.rsp.service.EventHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import java.net.URI;

import static java.lang.Thread.sleep;

@Component
public class ListenService implements CommandLineRunner {

    @Autowired
    private RSPConstant rspConstant;
    @Autowired
    private EventHandler handler;
    protected static final Logger logger = LoggerFactory.getLogger(ListenService.class);

    @Async("taskExecutor")
    @Override
    public void run(String... args) throws Exception {
        URI url = new URI(rspConstant.url);
        Configuration conf = new Configuration();
        HdfsAdmin hdfsAdmin = new HdfsAdmin(url, conf);
        DFSInotifyEventInputStream stream = hdfsAdmin.getInotifyEventStream();
        while(true){
            try {
                EventBatch batch = stream.take();
                if(batch != null){
                    for (Event event : batch.getEvents()) {
                        switch(event.getEventType()){
                            //Sent when a new file is created (including overwrite).
                            case CREATE:
                                Event.CreateEvent createEvent = (Event.CreateEvent) event;
                                handler.handleCreateEvent(createEvent);
                                if(createEvent.getPath().contains("zhaolingxiang"))
                                    logger.info("Event:CREATE, Path:======>  " + createEvent.getPath());
                                break;
                            //Sent when a file, directory, or symlink is renamed.
                            case RENAME:
                                Event.RenameEvent renameEvent = (Event.RenameEvent) event;
                                handler.handleRenameEvent(renameEvent);
                                if(renameEvent.getDstPath().contains("zhaolingxiang"))
                                    logger.info("Event:RENAME, Path:======>  "+  renameEvent.getDstPath());
                                break;
                            default: break;
                        }
                    }
                } else {
                    sleep(1000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

//    @Override
//    public void run(String... args) throws Exception {
//        URI url = new URI(rspConstant.url);
//        Configuration conf = new Configuration();
//        HdfsAdmin hdfsAdmin = new HdfsAdmin(url, conf);
//        DFSInotifyEventInputStream stream = hdfsAdmin.getInotifyEventStream();
//        new Thread(() -> {
//            while(true){
//                try {
//                    EventBatch batch = stream.take();
//                    if(batch != null){
//                        for (Event event : batch.getEvents()) {
//                            switch(event.getEventType()){
//                                //Sent when a new file is created (including overwrite).
//                                case CREATE:
//                                    Event.CreateEvent createEvent = (Event.CreateEvent) event;
//                                    handler.handleCreateEvent(createEvent);
//                                    if(createEvent.getPath().contains("zhaolingxiang"))
//                                        logger.info("Event:CREATE, Path:======>  " + createEvent.getPath());
//                                    break;
//                                //Sent when a file, directory, or symlink is renamed.
//                                case RENAME:
//                                    Event.RenameEvent renameEvent = (Event.RenameEvent) event;
//                                    handler.handleRenameEvent(renameEvent);
//                                    if(renameEvent.getDstPath().contains("zhaolingxiang"))
//                                        logger.info("Event:RENAME, Path:======>  "+  renameEvent.getDstPath());
//                                    break;
//                                //Sent when a file is closed after append or create.
////                                case CLOSE:
////                                    Event.CloseEvent closeEvent = (Event.CloseEvent) event;
////                                    if(!closeEvent.getPath().startsWith(rspPrefix)){
////                                        break;
////                                    }
////                                    handler.handleCloseEvent(closeEvent);
////                                    //TODO:根据路径，获取操作对象的相关数据，前置操作可能是创建或者追加
////                                    //long serviceTIme = closeEvent.getTimestamp();
////                                    if(closeEvent.getPath().contains("zhaolingxiang"))
////                                    logger.info("Event:CLOSE, Path:======>  "+ closeEvent.getPath());
////                                    break;
//                                //Sent when an existing file is opened for append.
////                                case APPEND:
////                                    //暂时不管
////                                    Event.AppendEvent appendEvent = (Event.AppendEvent) event;
////                                    logger.info("Event:APPEND, Path:======>  "+ appendEvent.getPath() );
////                                    break;
////                                //Sent when there is an update to directory or file
////                                // (none of the metadata tracked here applies to symlinks)
////                                // that is not associated with another inotify event.
////                                case METADATA:
////                                    //目录的元数据？
////                                    Event.MetadataUpdateEvent metadataUpdateEvent = (Event.MetadataUpdateEvent) event;
////                                    logger.info("Event:METADATA, Path:======>  "+ metadataUpdateEvent.getPath() );
////                                    break;
//                                //Sent when a file, directory, or symlink is deleted.
////                                case UNLINK:
////                                    Event.UnlinkEvent unlinkEvent = (Event.UnlinkEvent) event;
////                                    logger.info("Event:UNLINK, Path:======>  "+ unlinkEvent.getPath() );
////                                    break;
////                                //Sent when a file is truncated.
////                                case TRUNCATE:
////                                    //截断操作，暂时不管
////                                    Event.TruncateEvent truncateEvent= (Event.TruncateEvent) event;
////                                    logger.info("Event:TRUNCATE, Path:======>  "+ truncateEvent.getPath() );
////                                    break;
//                                default: break;
//                            }
//                        }
//                    } else {
//                        sleep(1000);
//                    }
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        }).start();
//     }
}
