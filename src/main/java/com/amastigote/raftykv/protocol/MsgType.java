package com.amastigote.raftykv.protocol;

/**
 * @author: hwding
 * @date: 2018/11/29
 */
@SuppressWarnings("JavaDoc")
public enum MsgType {
    HEARTBEAT,
    ELECT;

    /* ping or pong */
    public enum RpcAnalogType {
        REQ,
        RES
    }
}
