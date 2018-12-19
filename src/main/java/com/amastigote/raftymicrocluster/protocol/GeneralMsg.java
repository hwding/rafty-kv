package com.amastigote.raftymicrocluster.protocol;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

/**
 * @author: hwding
 * @date: 2018/11/29
 */
@SuppressWarnings("JavaDoc")
@Getter
@Setter
@ToString
public class GeneralMsg implements Serializable {
    private MsgType msgType = null;
    private MsgType.RpcAnalogType rpcAnalogType = null;

    private int term = -1;

    /* act as sender node id */
    private int responseToPort = -1;

    /* >> AppendEntryMsg only (ping) */
    private int prevLogIdx = -1;
    private int prevLogTerm = -1;
    private List<LogEntry> entries = null;
    private int leaderCommittedIdx = -1;
    /* << AppendEntryMsg only (ping) */

    /* >> AppendEntryMsg only (pong) */
    private int lastReplicatedLogIdx = -1;
    /* << AppendEntryMsg only (pong) */

    /* >> RequestVoteMsg only */
    private int lastLogTerm = -1;
    /* << RequestVoteMsg only */
}
