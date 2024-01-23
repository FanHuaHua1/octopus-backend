package com.szubd.rsp.websocket;

import com.szubd.rsp.WebSocketConnectService;
import org.apache.dubbo.config.annotation.DubboService;
import org.springframework.util.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Random;
import javax.websocket.*;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.concurrent.ConcurrentHashMap;

@Component
@DubboService
@ServerEndpoint(value = "/ss")
public class WebSocketServer implements WebSocketConnectService {
    private final static Logger log = LoggerFactory.getLogger(WebSocketServer.class);
    private Session session;

    /*    <sessionId, WebSocketServer> 用于存储websocket连接，key为sessionId  */
    private static ConcurrentHashMap<String, WebSocketServer> webSocketServerConcurrentHashMap = new ConcurrentHashMap();

    @OnOpen
    public void onOpen(Session session, EndpointConfig config) {
        this.session = session;
        webSocketServerConcurrentHashMap.put(session.getId(), this);
        System.out.println("WebSocket opened: " + session.getId());
    }

    /**
     * 连接关闭调用的方法
     */
    @OnClose
    public void onClose(Session session, CloseReason closeReason) {
        //从set中删除
        webSocketServerConcurrentHashMap.remove(session.getId());
        System.out.println("WebSocket closed: " + closeReason);
        log.info("连接关闭！");
    }

    /**
     * 收到客户端消息后调用的方法
     *
     * @param message 客户端发送过来的消息
     */
    @OnMessage
    public void onMessage(String message, Session session) {
        System.out.println("WebSocket message received: " + message);
        String dateStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis());
        try {
            //发送的消息也返回给当前连接，用于展示
            session.getBasicRemote().sendText(dateStr + "发送消息:" + message);

            //写入DB或者其他存储系统中。。。

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param session
     * @param error
     */
    @OnError
    public void onError(Session session, Throwable error) {
        log.error("发生错误");
        error.printStackTrace();
    }

    //@Scheduled(fixedRate = 5000)
    public void sendMessageToClient() {
        //没有连接时不做任何事情
        if (CollectionUtils.isEmpty(webSocketServerConcurrentHashMap)){
            return;
        }
        System.out.println("服务端发送消息到客户端");
        String dateStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis());
        long number = new Random().nextInt(10000);
        webSocketServerConcurrentHashMap.forEach((k, v) -> {
            try {
                v.session.getBasicRemote().sendText(dateStr + "收到消息:" + number);

                //写入DB或者其他存储系统中。。。

            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public void sendMessage(String message) throws IOException {
        if (CollectionUtils.isEmpty(webSocketServerConcurrentHashMap)){
            return;
        }
        System.out.println("服务端发送消息到客户端");
        String dateStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis());
        webSocketServerConcurrentHashMap.forEach((k, v) -> {
            try {
                String s = "<p>" + dateStr + "<p/><br><p>" + message + "<p/>";
                v.session.getBasicRemote().sendText(s);

            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public int sendMsg(String msg) {
        return 0;
    }

    @Override
    public void sendToRspMsg(String msg) {
        if (CollectionUtils.isEmpty(webSocketServerConcurrentHashMap)){
            return;
        }
        System.out.println("服务端发送消息到客户端");
        String dateStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis());
        webSocketServerConcurrentHashMap.forEach((k, v) -> {
            try {
                String s = "<p>" + dateStr + "<p/><br><p>" + msg + "<p/>";
                v.session.getBasicRemote().sendText(s);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
//
//    /**
//     * 群发自定义消息
//     */
//    public static void sendInfo(String message) throws IOException {
//        log.info(message);
//        for (WebSocketServer item : webSocketSet) {
//            try {
//                item.sendMessage(message);
//            } catch (IOException e) {
//                continue;
//            }
//        }
//    }
//
//    public static synchronized int getOnlineCount() {
//        return onlineCount;
//    }
//
//    public static synchronized void addOnlineCount() {
//        WebSocketServer.onlineCount++;
//    }
//
//    public static synchronized void subOnlineCount() {
//        WebSocketServer.onlineCount--;
//    }

}
