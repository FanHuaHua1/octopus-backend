package com.szubd.rsp.service.db;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;


@Component
public class connKeepAlive {
    @Autowired
    private JdbcTemplate template;

    @Scheduled(cron = "0/20 * * * * ?")
    public void keeyAlive() {
        template.execute("select 1");
    }
}