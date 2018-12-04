package com.amastigote.raftymicrocluster.protocol;

/**
 * @author: hwding
 * @date: 2018/11/29
 */
@SuppressWarnings("JavaDoc")
public class TimeSpan {
    public final static long HEARTBEAT_TIMEOUT_BASE = 4000L;
    public final static long ELECTION_TIMEOUT_BASE = 2500L;
    public final static long HEARTBEAT_TIMEOUT_ADDITIONAL_RANGE = 1500L;
    public final static long ELECTION_TIMEOUT_ADDITIONAL_RANGE = 1500L;
    public final static long HEARTBEAT_SEND_INTERVAL = 1500L;
}
