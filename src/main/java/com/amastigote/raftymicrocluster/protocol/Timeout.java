package com.amastigote.raftymicrocluster.protocol;

/**
 * @author: hwding
 * @date: 2018/11/29
 */
@SuppressWarnings("JavaDoc")
public class Timeout {
    public final static long HEARTBEAT_TIMEOUT_BASE = 6000L;
    public final static long ELECTION_TIMEOUT_BASE = 5000L;
    public final static long ELECTION_TIMEOUT_ADDITIONAL_RANGE = 2000L;
}
