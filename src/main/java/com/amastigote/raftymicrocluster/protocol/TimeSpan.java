package com.amastigote.raftymicrocluster.protocol;

import static com.amastigote.raftymicrocluster.conf.NodeGlobalConf.*;

/**
 * @author: hwding
 * @date: 2018/11/29
 */
@SuppressWarnings("JavaDoc")
public class TimeSpan {
    public final static long HEARTBEAT_TIMEOUT_BASE;
    public final static long ELECTION_TIMEOUT_BASE;
    public final static long HEARTBEAT_TIMEOUT_ADDITIONAL_RANGE;
    public final static long ELECTION_TIMEOUT_ADDITIONAL_RANGE;
    public final static long HEARTBEAT_SEND_INTERVAL;

    static {
        HEARTBEAT_TIMEOUT_BASE = Long.valueOf(readConf(KEY_HEARTBEAT_TIMEOUT_MIN_MILLIS));
        if (HEARTBEAT_TIMEOUT_BASE <= 0) {
            throw new IllegalArgumentException("min heartbeat timeout must be greater than 0");
        }

        ELECTION_TIMEOUT_BASE = Long.valueOf(readConf(KEY_ELECTION_TIMEOUT_MIN_MILLIS));
        if (ELECTION_TIMEOUT_BASE <= 0) {
            throw new IllegalArgumentException("min election timeout must be greater than 0");
        }

        HEARTBEAT_SEND_INTERVAL = Long.valueOf(readConf(KEY_HEARTBEAT_INTERVAL_MILLIS));
        if (HEARTBEAT_SEND_INTERVAL >= HEARTBEAT_TIMEOUT_BASE) {
            throw new IllegalArgumentException("heartbeat send interval must be less than min heartbeat timeout");
        }

        final long heartbeatMaxMillis = Long.valueOf(readConf(KEY_HEARTBEAT_TIMEOUT_MAX_MILLIS));
        if (heartbeatMaxMillis <= HEARTBEAT_TIMEOUT_BASE) {
            throw new IllegalArgumentException("max heartbeat timeout millis must be greater than min");
        }
        HEARTBEAT_TIMEOUT_ADDITIONAL_RANGE = heartbeatMaxMillis - HEARTBEAT_TIMEOUT_BASE;

        final long electionMaxMillis = Long.valueOf(readConf(KEY_ELECTION_TIMEOUT_MAX_MILLIS));
        if (electionMaxMillis <= ELECTION_TIMEOUT_BASE) {
            throw new IllegalArgumentException("max election timeout millis must be greater than  min");
        }
        ELECTION_TIMEOUT_ADDITIONAL_RANGE = electionMaxMillis - ELECTION_TIMEOUT_BASE;
    }

    /* trigger the static block above */
    public static void init() {
    }
}
