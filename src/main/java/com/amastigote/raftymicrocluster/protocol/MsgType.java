package com.amastigote.raftymicrocluster.protocol;

/**
 * @author: hwding
 * @date: 2018/11/29
 */
@SuppressWarnings("JavaDoc")
public enum MsgType {
    HEARTBEAT,
    ELECT;

    public enum ElectMsgType {
        VOTE_REQ,
        VOTE_RES
    }
}
