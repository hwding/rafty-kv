package com.amastigote.raftymicrocluster.protocol;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * @author: hwding
 * @date: 2018/11/29
 */
@SuppressWarnings("JavaDoc")
@Getter
@Setter
@ToString
public class GeneralMsg implements Serializable {
    private MsgType msgType;
    private MsgType.RpcAnalogType rpcAnalogType;

    private int term;

    /* act as sender node id */
    private int responseToPort;

    /* >> AppendEntryMsg only (ping) */
    private int prevLogIdx;
    private int prevLogTerm;
    private LogEntry[] entries;
    private int committedIdx;
    /* << AppendEntryMsg only (ping) */

    /* >> RequestVoteMsg only */
    /* >> AppendEntryMsg only (pong) */
    private int lastLogIdx;
    /* << AppendEntryMsg only (pong) */
    private int lastLogTerm;
    /* << RequestVoteMsg only */
}
