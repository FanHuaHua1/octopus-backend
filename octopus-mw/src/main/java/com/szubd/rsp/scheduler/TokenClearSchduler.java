package com.szubd.rsp.scheduler;

import com.szubd.rsp.service.user.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class TokenClearSchduler {
    @Autowired
    private UserService userService;
    private static final Logger logger = LoggerFactory.getLogger(TokenClearSchduler.class);

    /**
     * 每5分钟清理一次过期TOKEN
     */
    @Scheduled(cron = "0 */5 * * * ?")
    public void cleanExpiredToken() {
        logger.info("Clear all expired token");
        userService.cleanExpiredToken();
        logger.info("Complete clear expired token");
    }

}
