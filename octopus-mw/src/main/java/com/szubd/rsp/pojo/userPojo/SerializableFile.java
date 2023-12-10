package com.szubd.rsp.pojo.userPojo;

import lombok.Data;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.Serializable;

@Data
public class SerializableFile implements Serializable {
    private byte[] data;

    public SerializableFile(MultipartFile file) throws IOException {
        data = file.getBytes().clone();
    }
}
