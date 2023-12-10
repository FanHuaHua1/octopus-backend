package com.szubd.rsp.mapper;

import com.szubd.rsp.pojo.algoPojo.AlgorithmInfo;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface UserAlgoMapper {
    AlgorithmInfo getAlgorithmById(Long algoId);
}
