package com.szubd.rsp.algo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class RspParams{
    public String originName;
    public String rspName;
    public Integer blockNum;
    public String originType;
    public String nodeId;
}
