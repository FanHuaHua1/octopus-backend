package com.szubd.rsp.service.user;

import com.szubd.rsp.user.algoPojo.AlgorithmInfo;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface UserAlgoMapper {
    AlgorithmInfo getAlgorithmById(Long algoId);
}
