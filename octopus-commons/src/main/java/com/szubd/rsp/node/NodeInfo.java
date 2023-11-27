package com.szubd.rsp.node;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class NodeInfo implements Serializable {
    private int id;
    private String ip;
    private String prefix;
    private String nameNodeIP;
    private String clusterName;
}
