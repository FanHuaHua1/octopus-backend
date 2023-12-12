package com.szubd.rsp.filter;

import com.szubd.rsp.exception.TokenException;
import com.szubd.rsp.service.user.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.szubd.rsp.utils.JwtUtils;
import org.springframework.stereotype.Component;

import javax.servlet.*;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


@Component
@WebFilter(urlPatterns = "/*", filterName = "TokenFilter")
public class TokenFilter implements Filter {
    @Autowired
    private UserService userService;

    private static final Logger logger = LoggerFactory.getLogger(TokenFilter.class);

    @Autowired
    private JwtUtils jwtUtils;

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        if(request.getMethod().equals("OPTIONS")){
            filterChain.doFilter(servletRequest, servletResponse);
            return;
        }
        String authorization = request.getHeader("Authorization");
        logger.info("Filter Authorization = " + authorization);
        logger.info("Check request uri = " + request.getRequestURI());

        List<String> free_uri_list = new ArrayList<>();
        free_uri_list.add("/v1/user/loginUser");
        free_uri_list.add("/v1/user/registerUser");
        // 自动放行请求
        if (free_uri_list.contains(request.getRequestURI())) {
            filterChain.doFilter(servletRequest, servletResponse);
            return;
        }

        // 检查TOKEN信息
        // filter中不能用统一异常处理
        if (authorization == null) {
            ((HttpServletResponse) servletResponse).sendError(4001, "Authorization Failed - Null Authorization");
            return;
//            throw ExceptionEnum.exception(ExceptionEnum.NULL_TOKEN);
        }
        try {
            authorization = authorization.replace("Bearer ", "");
            Boolean isCheck = userService.checkRawToken(authorization);
            if (!isCheck) {
                ((HttpServletResponse) servletResponse).sendError(500, "UNKNOWN ERROR");
                return;
            }
        } catch (TokenException e) {
            logger.error(e.toString());
//            throw e;
            ((HttpServletResponse) servletResponse).sendError(e.getExceptionCode(), e.getExceptionInfo());
            return;
        }

        // 通过前面的所有filter仍未return，最终放行
        filterChain.doFilter(servletRequest, servletResponse);
    }
}
