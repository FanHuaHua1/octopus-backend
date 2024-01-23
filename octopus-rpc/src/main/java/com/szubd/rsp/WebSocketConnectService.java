package com.szubd.rsp;

public interface WebSocketConnectService {
    int sendMsg(String msg);

    void sendToRspMsg(String msg);
}
