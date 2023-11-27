package com.szubd.rsp.constants;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class JobConstant {
    @Value("${yarn.rm}")
    public String rmUrl;
}
