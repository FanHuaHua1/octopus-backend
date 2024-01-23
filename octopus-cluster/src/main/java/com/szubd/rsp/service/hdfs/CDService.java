package com.szubd.rsp.service.hdfs;

import com.szubd.rsp.constants.RSPConstant;
import com.szubd.rsp.file.CDDubboService;
import com.szubd.rsp.file.GlobalRSPInfo;
import com.szubd.rsp.file.LocalRSPInfo;
import com.szubd.rsp.file.OriginInfo;
import com.szubd.rsp.mapper.GlobalRSPInfoMapper;
import com.szubd.rsp.mapper.LocalRSPInfoMapper;
import com.szubd.rsp.mapper.OriginInfoMapper;
import org.apache.dubbo.config.annotation.DubboService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.URI;

@DubboService
@Component
public class CDService implements CDDubboService {
    @Autowired
    private RSPConstant rspConstant;
    @Autowired
    private LocalRSPInfoMapper localRSPInfoMapper;
    @Autowired
    private GlobalRSPInfoMapper globalRSPInfoMapper;
    @Autowired
    private OriginInfoMapper originInfoMapper;

    protected static final Logger logger = LoggerFactory.getLogger(CDService.class);
    public boolean rmFile(int type, String suffix) throws Exception {
        if(type != 0 && type != 1 && type != 2){
            return false;
        }
        String url = "";
        switch (type){
            case 0:
                url = rspConstant.url + rspConstant.originPrefix + suffix;
                break;
            case 1:
                url = rspConstant.url + rspConstant.localRspPrefix + suffix;
                break;
            case 2:
                url = rspConstant.url + rspConstant.globalRspPrefix + suffix;
                break;
        }
        FileSystem fileSystem = rspConstant.getSuperFileSystem();
        boolean isSuccess = fileSystem.delete(new Path(url));
        if(isSuccess){
            String[] split = suffix.split("/");
            switch (type){
                case 0:
                    originInfoMapper.deleteInfo(new OriginInfo(split[0], split[1], -1));
                    break;
                case 1:
                    localRSPInfoMapper.deleteInfo(new LocalRSPInfo(split[0], split[1], -1));
                    break;
                case 2:
                    globalRSPInfoMapper.deleteInfo(new GlobalRSPInfo(split[0], split[1], -1));
                    break;
            }
        }
        return isSuccess;
    }
}
