package com.amastigote.raftykv.protocol;

/**
 * @author: hwding
 * @date: 2018/11/29
 */
@SuppressWarnings("JavaDoc")
public enum Role {
    FOLLOWER,
    LEADER,
    CANDIDATE
}