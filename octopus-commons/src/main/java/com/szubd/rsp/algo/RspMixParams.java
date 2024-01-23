package com.szubd.rsp.algo;

import com.szubd.rsp.file.OriginInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RspMixParams {
    public List<OriginInfo> data;
    public String blockRatio;
    public String repartitionNum;
    public String mixType;

    @Override
    public String toString() {
        return "RspMixParams{" +
                "data=" + data +
                ", blockRatio='" + blockRatio + '\'' +
                ", repartitionNum='" + repartitionNum + '\'' +
                ", mixType='" + mixType + '\'' +
                '}';
    }
}
