package com.szubd.rsp.algo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class AlgoInfo implements Serializable {
    private int id;
    private String name;
    private String params;
    private String type;

}
