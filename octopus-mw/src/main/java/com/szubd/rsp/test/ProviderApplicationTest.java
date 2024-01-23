package com.szubd.rsp.test;

import com.szubd.rsp.file.OriginInfo;
import com.szubd.rsp.service.node.NodeInfoMapper;
import com.szubd.rsp.service.file.origin.OriginInfoMapper;
import com.szubd.rsp.node.NodeInfo;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

@SpringBootTest
public class ProviderApplicationTest {
    @Autowired
    OriginInfoMapper originInfoMapper;
    @Autowired
    NodeInfoMapper nodeMapper;
    @Autowired
    DataSource dataSource;
    protected static final Logger logger = LoggerFactory.getLogger(ProviderApplicationTest.class);
    @Test
    public void testDB() throws SQLException {
        logger.info(dataSource.toString());
        Connection connection = dataSource.getConnection();
        logger.info("===============> " + connection);

        //template模板，拿来即用
        connection.close();
    }

    @Test
    public void testMapper(){
        List<OriginInfo> originInfos = originInfoMapper.queryAll();
        logger.info("========================");
        logger.info(originInfos.toString());
        logger.info("========================");
    }

//    @Test
//    public void testCreate(){
//
//        System.out.println("========================");
//        System.out.println(rspInfoMapper.createHDFSFile(new RSPInfo(0, "student2", "student",45, 43, 0, true, true, true, true)));
//        List<RSPInfo> rspInfos = rspInfoMapper.queryAll();
//        System.out.println(rspInfos);
//        System.out.println("========================");
//    }


    @Test
    public void testCreate2(){
        NodeInfo nodeInfo = new NodeInfo();
        nodeInfo.setIp("sdasdasd");
        nodeInfo.setPrefix("sdasdasd");
//        logger.info(nodeMapper.insertNewNode(nodeInfo));
//        logger.info(nodeInfo);
    }

    @Test
    public void testQuery(){
        System.out.println(nodeMapper.queryForNodeId("sdasdas333d"));
        System.out.println("========================");
    }
}
