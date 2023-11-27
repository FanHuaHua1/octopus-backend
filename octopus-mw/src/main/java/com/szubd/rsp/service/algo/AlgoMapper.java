package com.szubd.rsp.service.algo;

import com.szubd.rsp.algo.AlgoInfo;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

import java.util.List;

@Mapper
@Repository
public interface AlgoMapper {
    List<AlgoInfo> listAllAlgoInfo();
    int addAlgo(AlgoInfo algoInfo);

}
