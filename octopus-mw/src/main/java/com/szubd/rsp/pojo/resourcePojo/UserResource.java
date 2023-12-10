package com.szubd.rsp.pojo.resourcePojo;

import com.szubd.rsp.pojo.constant.UserResourceConstant;
import lombok.Data;

import java.io.Serializable;

@Data
public class UserResource implements Serializable {
    private Long id;
    private String userId;
    private Long memory;
    private Integer cpu;
    private Long hardDisk;

    public UserResource() {
    }

    /**
     * Init new resource of new user
     * @param userId
     */
    public UserResource(String userId) {
        this.setUserId(userId);
        this.setMemory(UserResourceConstant.INIT_MEMORY);
        this.setCpu(UserResourceConstant.INIT_CPU);
        this.setHardDisk(UserResourceConstant.INIT_HARD_DISK);
    }

    public UserResource update(UserResource userResource) {
        if (userResource.cpu != null) {
            this.setCpu(userResource.cpu);
        }
        if (userResource.memory != null) {
            this.setMemory(userResource.memory);
        }
        if (userResource.hardDisk != null) {
            this.setHardDisk(userResource.hardDisk);
        }
        return this;
    }

}
