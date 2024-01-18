package com.szubd.rsp.user.algoPojo;

import lombok.Data;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Data
public class AlgorithmInfo {
    private Long id;
    private String name;
    private String params;
    private String type;
    private String algoPermission;

    public Boolean checkUserPermission(Integer userType) {
        return getPermission().contains(userType);
    }

    private List<Integer> getPermission() {
        return Arrays.stream(algoPermission.split(","))
                .map(permission -> Integer.valueOf(permission))
                .collect(Collectors.toList());
    }
}
