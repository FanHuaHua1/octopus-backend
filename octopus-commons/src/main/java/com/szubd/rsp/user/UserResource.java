package com.szubd.rsp.user;

import com.szubd.rsp.user.constant.UserResourceConstant;
import lombok.Data;

import java.io.Serializable;

@Data
public class UserResource implements Serializable {
    private Long id;
    private String userId;
    private Long memory;
    private Integer cpu;
    private Long hardDisk;
    private Long useMemory;
    private Integer useCpu;
    private Long useHardDisk;
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
        this.setUseMemory(0L);
        this.setUseCpu(0);
        this.setUseHardDisk(0L);
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
        if (userResource.useCpu != null) {
            this.setUseCpu(userResource.cpu);
        }
        if (userResource.useMemory != null) {
            this.setUseMemory(userResource.memory);
        }
        if (userResource.useHardDisk != null) {
            this.setUseHardDisk(userResource.hardDisk);
        }
        return this;
    }


}
