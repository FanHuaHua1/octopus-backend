package com.szubd.rsp.sparkInfo.clusterInfo;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @projectName: octopus-web
 * @package: com.szubd.rsp.sparkInfo.clusterInfo
 * @className: AppIdTree
 * @author: Chandler
 * @description: 用于格式化AppIdList，直接以树的格式返回给前端使用
 * @date: 2023/8/23 下午 4:13
 * @version: 2.0
 */
@Data
public class AppIdTree implements Serializable {
    private static final long serialVersionUID = 10003L;
    private String label;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<AppIdTree> children;

    public AppIdTree(String label) {
        this.label = label;
    }

    public void addChildren(AppIdTree tree){
        if (this.children==null){
            this.children = new ArrayList<>();
        }
        this.children.add(tree);
    }

    // 以树形结构打印，测试用
    public void printTree(){
        this.printTree(this, "");
    }

    private void printTree(AppIdTree tree, String prefix){
        if (tree==null) {
            return;
        }
        System.out.println(prefix + tree.label);
        if (tree.children==null) {
            return;
        }
        for (AppIdTree child : tree.children) {
            printTree(child,prefix+'-');
        }
    }
}
