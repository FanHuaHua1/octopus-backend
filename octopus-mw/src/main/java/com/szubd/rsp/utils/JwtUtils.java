package com.szubd.rsp.utils;

import io.jsonwebtoken.*;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.Map;
import java.util.Timer;

@Component
public class JwtUtils {
    //签名私钥
    private static final String key = "szubd_LOGO";
    @Value("${token.expiredHour:1}")
    private Integer expiredHour;
    private static final Logger logger = LoggerFactory.getLogger(JwtUtils.class);

    /**
     * 设置认证token
     *
     * @param id 用户登录ID
     * @param subject 用户登录名
     * @param map 其他私有数据
     * @return
     */
    public String createJwt(String id, String subject, Map<String, Object> map) {
        //1、设置失效时间
        long now = System.currentTimeMillis(); //毫秒
        logger.info("Token of user {} created at {}", id, Time.formatTime(now));
        Long failureTime = expiredHour * 60 * 60 * 1000L;
        long exp = now + failureTime;
        logger.info("Token will expired in {} hour(s), i.e., {}", expiredHour, Time.formatTime(exp));

        //2、创建JwtBuilder
        JwtBuilder jwtBuilder = Jwts.builder()
                // userId
                .setId(id)
                // userName
                .setSubject(subject)
                .setIssuedAt(new Date())
                .signWith(SignatureAlgorithm.HS256, key); //设置签名HS256加密

        //3、根据map{其他数据}设置claims
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            jwtBuilder.claim(entry.getKey(), entry.getValue());
        }
        jwtBuilder.setExpiration(new Date(exp));

        //4、创建token
        String token = jwtBuilder.compact();
        return token;
    }

    /**
     * 解析token
     *
     * @param token
     * @return
     */
    public Claims parseJwt(String token) {
        Claims claims;
        try {
            claims = Jwts.parser()
                    .setSigningKey(key)
                    .parseClaimsJws(token)
                    .getBody();
        } catch (ExpiredJwtException e) {
            return null;
        }
        return claims;
    }

}