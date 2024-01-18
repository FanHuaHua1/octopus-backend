package com.szubd.rsp.service.user;

import com.szubd.rsp.user.UserAlgoDubboService;
import com.szubd.rsp.user.UserInfo;
import com.szubd.rsp.user.algoPojo.AlgorithmInfo;
import org.apache.dubbo.config.annotation.DubboService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@DubboService
@Component
public class UserAlgoService implements UserAlgoDubboService {
    @Autowired
    private UserAlgoMapper algoMapper;
    @Autowired
    private UserInfoMapper userInfoMapper;

    @Override
    public Boolean checkUserAlgo(String userId, Long algoId) {
        AlgorithmInfo algo = algoMapper.getAlgorithmById(algoId);
        UserInfo user = userInfoMapper.queryUserByUserId(userId);
        if (algo == null) return false;
        if (user == null) return false;
        // 目前数据库字段的类型还没统一，先转换一下
        return algo.checkUserPermission(Integer.valueOf(user.getUserPrivilege()));
    }
}
